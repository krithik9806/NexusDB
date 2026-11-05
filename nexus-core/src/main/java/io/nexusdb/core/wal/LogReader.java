package io.nexusdb.core.wal;

import io.nexusdb.core.storage.StorageException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Sequential Write-Ahead Log (WAL) reader that verifies record framing and CRC32 checksums,
 * stopping cleanly (and optionally truncating the file) when encountering a torn write or
 * corrupted tail.
 *
 * <p>Crash-Safety Invariant:
 * An OS or process crash during a WAL append may leave a partial header, a truncated payload,
 * zero-filled trailing bytes, or a record whose CRC32 does not match. {@link LogReader} detects
 * any of these conditions at record boundary {@code validEndOffset} and halts scanning without
 * throwing, optionally truncating the WAL file to {@code validEndOffset} so subsequent appends
 * start on a clean record boundary.
 */
public final class LogReader implements Iterable<LogRecord>, AutoCloseable {
    private final Path walPath;
    private final boolean truncateCorruptTail;
    private final FileChannel channel;
    private long position = 0L;
    private long validEndOffset = 0L;
    private boolean corruptedTailDetected = false;
    private boolean closed = false;

    public LogReader(Path walPath) {
        this(walPath, false);
    }

    public LogReader(Path walPath, boolean truncateCorruptTail) {
        this.walPath = Objects.requireNonNull(walPath, "walPath must not be null");
        this.truncateCorruptTail = truncateCorruptTail;
        try {
            if (!Files.exists(walPath)) {
                this.channel = null;
            } else if (truncateCorruptTail) {
                this.channel = FileChannel.open(walPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } else {
                this.channel = FileChannel.open(walPath, StandardOpenOption.READ);
            }
        } catch (IOException e) {
            throw new StorageException("Failed to open WAL file for reading: " + walPath, e);
        }
    }

    /**
     * Reads all valid {@link LogRecord}s from {@code walPath}, stopping cleanly at any torn or corrupt tail.
     */
    public static List<LogRecord> readAll(Path walPath) {
        return readAll(walPath, false);
    }

    /**
     * Reads all valid {@link LogRecord}s from {@code walPath}, stopping cleanly at any torn or corrupt tail
     * and truncating the file to the last valid record boundary when {@code truncateCorruptTail} is {@code true}.
     */
    public static List<LogRecord> readAll(Path walPath, boolean truncateCorruptTail) {
        List<LogRecord> records = new ArrayList<>();
        try (LogReader reader = new LogReader(walPath, truncateCorruptTail)) {
            LogRecord record;
            while ((record = reader.nextRecord()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    /**
     * Reads and validates the next sequential {@link LogRecord}, or returns {@code null} on EOF
     * or upon encountering a torn/corrupt tail.
     */
    public LogRecord nextRecord() {
        if (closed || channel == null) {
            return null;
        }
        try {
            long fileSize = channel.size();
            if (position >= fileSize) {
                return null;
            }
            if (fileSize - position < LogRecord.HEADER_SIZE) {
                handleTornOrCorruptTail();
                return null;
            }

            ByteBuffer header = ByteBuffer.allocate(LogRecord.HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
            if (!readFullyOrFalse(channel, header, position)) {
                handleTornOrCorruptTail();
                return null;
            }
            header.flip();

            int magic = header.getInt(0);
            int payloadLen = header.getInt(4);
            int expectedCrc = header.getInt(8);

            if (magic != LogRecord.MAGIC
                    || payloadLen < LogRecord.MIN_PAYLOAD_SIZE
                    || payloadLen > LogRecord.MAX_PAYLOAD_SIZE
                    || position + LogRecord.HEADER_SIZE + payloadLen > fileSize) {
                handleTornOrCorruptTail();
                return null;
            }

            ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.BIG_ENDIAN);
            if (!readFullyOrFalse(channel, payload, position + LogRecord.HEADER_SIZE)) {
                handleTornOrCorruptTail();
                return null;
            }
            payload.flip();

            int actualCrc = LogRecord.computeCrc32(header, payload);
            if (actualCrc != expectedCrc) {
                handleTornOrCorruptTail();
                return null;
            }

            LogRecord record;
            try {
                record = LogRecord.deserializePayload(payload, 0, payloadLen);
            } catch (RuntimeException e) {
                handleTornOrCorruptTail();
                return null;
            }

            position += LogRecord.HEADER_SIZE + payloadLen;
            validEndOffset = position;
            return record;
        } catch (IOException e) {
            throw new StorageException("I/O error while reading WAL file: " + walPath, e);
        }
    }

    private void handleTornOrCorruptTail() throws IOException {
        corruptedTailDetected = true;
        validEndOffset = position;
        position = channel.size();
        if (truncateCorruptTail) {
            channel.truncate(validEndOffset);
            channel.force(false);
        }
    }

    private static boolean readFullyOrFalse(FileChannel ch, ByteBuffer dst, long offset) throws IOException {
        long pos = offset;
        while (dst.hasRemaining()) {
            int read = ch.read(dst, pos);
            if (read < 0) {
                return false;
            }
            pos += read;
        }
        return true;
    }

    public long getValidEndOffset() {
        return validEndOffset;
    }

    public boolean isCorruptedTailDetected() {
        return corruptedTailDetected;
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return new Iterator<>() {
            private LogRecord next = nextRecord();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public LogRecord next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                LogRecord current = next;
                next = nextRecord();
                return current;
            }
        };
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new StorageException("Failed to close WAL reader channel: " + walPath, e);
                }
            }
        }
    }
}
