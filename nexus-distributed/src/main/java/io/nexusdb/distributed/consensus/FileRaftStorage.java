package io.nexusdb.distributed.consensus;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Crash-safe file-based implementation of {@link RaftStorage} using CRC32-framed binary records
 * and {@link FileChannel#force(boolean)} ({@code fsync}) on every state transition.
 *
 * <p>On-Disk Record Framing & Crash-Safety Invariant:
 * <ul>
 *   <li>File header: 8-byte magic {@code NXRAFT01}.</li>
 *   <li>Sequential records, each framed as:
 *       {@code [int32 payloadLength][int32 crc32][byte recordType][byte[] payload]}</li>
 *   <li>Record types:
 *     <ul>
 *       <li>{@code 1 (TERM_AND_VOTE)}: {@code [int64 currentTerm][int32 votedForLen (-1 if null)][utf8Bytes]}</li>
 *       <li>{@code 2 (APPEND_ENTRY)}: {@code [int64 index][int64 term][int32 cmdLen][cmdBytes]}</li>
 *       <li>{@code 3 (TRUNCATE_FROM)}: {@code [int64 fromIndex]}</li>
 *     </ul>
 *   </li>
 *   <li>During {@link #load()}, records are replayed sequentially. If a power loss or crash left a torn
 *       or corrupted trailing record at the end of the file (incomplete bytes or CRC32 mismatch), the file
 *       is cleanly truncated back to the last valid record offset.</li>
 * </ul>
 */
public final class FileRaftStorage implements RaftStorage {
    private static final byte[] MAGIC = "NXRAFT01".getBytes(StandardCharsets.US_ASCII);
    private static final byte RECORD_TERM_AND_VOTE = 1;
    private static final byte RECORD_APPEND_ENTRY = 2;
    private static final byte RECORD_TRUNCATE_FROM = 3;

    private final Path filePath;
    private FileChannel channel;

    public FileRaftStorage(Path filePath) {
        this.filePath = Objects.requireNonNull(filePath, "filePath must not be null");
        openChannel();
    }

    private void openChannel() {
        try {
            if (filePath.getParent() != null) {
                Files.createDirectories(filePath.getParent());
            }
            this.channel = FileChannel.open(
                    filePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            if (channel.size() == 0L) {
                writeAll(ByteBuffer.wrap(MAGIC));
                channel.force(true);
            } else if (channel.size() < MAGIC.length) {
                throw new IllegalStateException("Corrupted Raft storage file header: " + filePath);
            } else {
                ByteBuffer header = ByteBuffer.allocate(MAGIC.length);
                channel.position(0L);
                readFully(header);
                for (int i = 0; i < MAGIC.length; i++) {
                    if (header.get(i) != MAGIC[i]) {
                        throw new IllegalStateException("Invalid magic header in Raft storage file: " + filePath);
                    }
                }
                channel.position(channel.size());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open FileRaftStorage at " + filePath, e);
        }
    }

    @Override
    public synchronized PersistentState load() {
        try {
            long fileSize = channel.size();
            long pos = MAGIC.length;
            long currentTerm = 0L;
            String votedFor = null;
            List<RaftLogEntry> log = new ArrayList<>();

            ByteBuffer headerBuf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
            while (pos < fileSize) {
                if (pos + 8 > fileSize) {
                    // Torn trailing frame header -> truncate to last valid offset
                    channel.truncate(pos);
                    channel.force(true);
                    break;
                }
                headerBuf.clear();
                channel.position(pos);
                readFully(headerBuf);
                headerBuf.flip();
                int payloadLen = headerBuf.getInt();
                int expectedCrc = headerBuf.getInt();

                if (payloadLen <= 0 || pos + 8L + payloadLen > fileSize) {
                    // Torn trailing payload -> truncate to last valid offset
                    channel.truncate(pos);
                    channel.force(true);
                    break;
                }

                ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen).order(ByteOrder.BIG_ENDIAN);
                readFully(payloadBuf);
                byte[] payloadBytes = payloadBuf.array();

                CRC32 crc = new CRC32();
                crc.update(payloadBytes, 0, payloadLen);
                if ((int) crc.getValue() != expectedCrc) {
                    // Corrupted trailing frame -> truncate to last valid offset
                    channel.truncate(pos);
                    channel.force(true);
                    break;
                }

                payloadBuf.flip();
                byte recordType = payloadBuf.get();
                switch (recordType) {
                    case RECORD_TERM_AND_VOTE -> {
                        currentTerm = payloadBuf.getLong();
                        int voteLen = payloadBuf.getInt();
                        if (voteLen < 0) {
                            votedFor = null;
                        } else {
                            byte[] voteBytes = new byte[voteLen];
                            payloadBuf.get(voteBytes);
                            votedFor = new String(voteBytes, StandardCharsets.UTF_8);
                        }
                    }
                    case RECORD_APPEND_ENTRY -> {
                        long index = payloadBuf.getLong();
                        long term = payloadBuf.getLong();
                        int cmdLen = payloadBuf.getInt();
                        byte[] cmd = new byte[cmdLen];
                        payloadBuf.get(cmd);
                        if (index != log.size() + 1L) {
                            throw new IllegalStateException(
                                    "Corrupted Raft log index sequence: expected " + (log.size() + 1L) + ", got " + index);
                        }
                        log.add(new RaftLogEntry(index, term, cmd));
                    }
                    case RECORD_TRUNCATE_FROM -> {
                        long fromIndex = payloadBuf.getLong();
                        while (log.size() >= fromIndex && !log.isEmpty()) {
                            log.remove(log.size() - 1);
                        }
                    }
                    default -> throw new IllegalStateException("Unknown RaftStorage record type: " + recordType);
                }

                pos += 8L + payloadLen;
            }

            channel.position(channel.size());
            return new PersistentState(currentTerm, votedFor, log);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load FileRaftStorage from " + filePath, e);
        }
    }

    @Override
    public synchronized void saveTermAndVote(long currentTerm, String votedFor) {
        byte[] voteBytes = votedFor == null ? null : votedFor.getBytes(StandardCharsets.UTF_8);
        int payloadLen = 1 + 8 + 4 + (voteBytes == null ? 0 : voteBytes.length);
        ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.BIG_ENDIAN);
        payload.put(RECORD_TERM_AND_VOTE);
        payload.putLong(currentTerm);
        if (voteBytes == null) {
            payload.putInt(-1);
        } else {
            payload.putInt(voteBytes.length);
            payload.put(voteBytes);
        }
        writeFramedRecord(payload.array(), true);
    }

    @Override
    public synchronized void appendEntries(List<RaftLogEntry> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        if (entries.isEmpty()) {
            return;
        }
        for (int i = 0; i < entries.size(); i++) {
            RaftLogEntry entry = entries.get(i);
            byte[] cmd = entry.command();
            int payloadLen = 1 + 8 + 8 + 4 + cmd.length;
            ByteBuffer payload = ByteBuffer.allocate(payloadLen).order(ByteOrder.BIG_ENDIAN);
            payload.put(RECORD_APPEND_ENTRY);
            payload.putLong(entry.index());
            payload.putLong(entry.term());
            payload.putInt(cmd.length);
            payload.put(cmd);
            boolean fsyncNow = (i == entries.size() - 1);
            writeFramedRecord(payload.array(), fsyncNow);
        }
    }

    @Override
    public synchronized void truncateAndAppend(long fromIndex, List<RaftLogEntry> newEntries) {
        if (fromIndex < 1) {
            throw new IllegalArgumentException("fromIndex must be >= 1");
        }
        Objects.requireNonNull(newEntries, "newEntries must not be null");
        ByteBuffer truncPayload = ByteBuffer.allocate(1 + 8).order(ByteOrder.BIG_ENDIAN);
        truncPayload.put(RECORD_TRUNCATE_FROM);
        truncPayload.putLong(fromIndex);
        writeFramedRecord(truncPayload.array(), newEntries.isEmpty());
        if (!newEntries.isEmpty()) {
            appendEntries(newEntries);
        }
    }

    private void writeFramedRecord(byte[] payloadBytes, boolean fsync) {
        CRC32 crc = new CRC32();
        crc.update(payloadBytes, 0, payloadBytes.length);
        int crcVal = (int) crc.getValue();

        ByteBuffer frame = ByteBuffer.allocate(8 + payloadBytes.length).order(ByteOrder.BIG_ENDIAN);
        frame.putInt(payloadBytes.length);
        frame.putInt(crcVal);
        frame.put(payloadBytes);
        frame.flip();

        try {
            channel.position(channel.size());
            writeAll(frame);
            if (fsync) {
                channel.force(false);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write record to FileRaftStorage at " + filePath, e);
        }
    }

    private void writeAll(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    private void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int r = channel.read(buf);
            if (r < 0) {
                break;
            }
        }
    }

    public Path getFilePath() {
        return filePath;
    }

    @Override
    public synchronized void close() {
        if (channel != null && channel.isOpen()) {
            try {
                channel.force(true);
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close FileRaftStorage at " + filePath, e);
            }
        }
    }
}
