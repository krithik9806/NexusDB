package io.nexusdb.core.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages fixed-size page I/O on a database file using Java NIO {@link FileChannel} positional operations.
 *
 * <p>Concurrency & Crash-Safety Invariants:
 * <ul>
 *   <li>Positional {@code FileChannel.read(buf, pos)} and {@code FileChannel.write(buf, pos)} are thread-safe
 *       for concurrent reads/writes to distinct pages without holding a global file lock.</li>
 *   <li>Header metadata mutations ({@link #allocatePage()}, free-list head, catalog page ID, checkpoint LSN)
 *       are synchronized on {@code headerLock} and immediately persisted to Page 0.</li>
 * </ul>
 */
public final class DiskManager implements AutoCloseable {
    public static final int MAGIC_NUMBER = 0x4E584442; // "NXDB"
    public static final int FORMAT_VERSION = 1;
    public static final int DEFAULT_PAGE_SIZE = 4096;
    public static final int MIN_PAGE_SIZE = 512;

    public static final int HEADER_MAGIC_OFFSET = 0;
    public static final int HEADER_VERSION_OFFSET = 4;
    public static final int HEADER_PAGE_SIZE_OFFSET = 8;
    public static final int HEADER_PAGE_COUNT_OFFSET = 12;
    public static final int HEADER_FREE_LIST_HEAD_OFFSET = 16;
    public static final int HEADER_CATALOG_PAGE_ID_OFFSET = 20;
    public static final int HEADER_CHECKPOINT_LSN_OFFSET = 24;

    private final Path filePath;
    private final FileChannel channel;
    private final int pageSize;
    private final AtomicInteger pageCount;
    private volatile int freeListHead;
    private volatile int catalogMetaPageId;
    private volatile long checkpointLsn;
    private volatile boolean closed;
    private final Object headerLock = new Object();

    private DiskManager(
            Path filePath,
            FileChannel channel,
            int pageSize,
            int pageCount,
            int freeListHead,
            int catalogMetaPageId,
            long checkpointLsn) {
        this.filePath = filePath;
        this.channel = channel;
        this.pageSize = pageSize;
        this.pageCount = new AtomicInteger(pageCount);
        this.freeListHead = freeListHead;
        this.catalogMetaPageId = catalogMetaPageId;
        this.checkpointLsn = checkpointLsn;
    }

    public static DiskManager open(Path filePath) {
        return open(filePath, DEFAULT_PAGE_SIZE);
    }

    public static DiskManager open(Path filePath, int configuredPageSize) {
        validatePageSize(configuredPageSize);
        try {
            if (filePath.getParent() != null) {
                Files.createDirectories(filePath.getParent());
            }
            boolean exists = Files.exists(filePath) && Files.size(filePath) > 0;
            FileChannel channel = FileChannel.open(
                    filePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            if (!exists) {
                DiskManager dm = new DiskManager(filePath, channel, configuredPageSize, 1, -1, -1, 0L);
                dm.writeHeaderPage();
                dm.fsync();
                return dm;
            } else {
                try {
                    long fileSize = channel.size();
                    if (fileSize < MIN_PAGE_SIZE) {
                        throw new StorageException("Corrupted database file: file size too small (" + fileSize + " bytes)");
                    }
                    ByteBuffer probe = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
                    readFully(channel, probe, 0L);
                    probe.flip();

                    int magic = probe.getInt(HEADER_MAGIC_OFFSET);
                    if (magic != MAGIC_NUMBER) {
                        throw new StorageException(String.format("Invalid magic number: 0x%08X", magic));
                    }
                    int version = probe.getInt(HEADER_VERSION_OFFSET);
                    if (version != FORMAT_VERSION) {
                        throw new StorageException("Unsupported format version: " + version);
                    }
                    int storedPageSize = probe.getInt(HEADER_PAGE_SIZE_OFFSET);
                    validatePageSize(storedPageSize);
                    int storedPageCount = probe.getInt(HEADER_PAGE_COUNT_OFFSET);
                    if (storedPageCount < 1 || (long) storedPageCount * storedPageSize > fileSize) {
                        throw new StorageException("Invalid page count in header: " + storedPageCount);
                    }
                    int freeListHead = probe.getInt(HEADER_FREE_LIST_HEAD_OFFSET);
                    int catalogMetaPageId = probe.getInt(HEADER_CATALOG_PAGE_ID_OFFSET);
                    long checkpointLsn = probe.getLong(HEADER_CHECKPOINT_LSN_OFFSET);

                    return new DiskManager(
                            filePath,
                            channel,
                            storedPageSize,
                            storedPageCount,
                            freeListHead,
                            catalogMetaPageId,
                            checkpointLsn);
                } catch (RuntimeException | IOException e) {
                    channel.close();
                    if (e instanceof StorageException se) {
                        throw se;
                    }
                    throw new StorageException("Failed to open database file: " + filePath, e);
                }
            }
        } catch (IOException e) {
            throw new StorageException("I/O error opening database file: " + filePath, e);
        }
    }

    private static void validatePageSize(int size) {
        if (size < MIN_PAGE_SIZE || Integer.bitCount(size) != 1) {
            throw new StorageException("Page size must be a power of 2 and >= " + MIN_PAGE_SIZE + ", got: " + size);
        }
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getPageCount() {
        return pageCount.get();
    }

    public int getFreeListHead() {
        return freeListHead;
    }

    public void setFreeListHead(int freeListHead) {
        synchronized (headerLock) {
            ensureOpen();
            this.freeListHead = freeListHead;
            writeHeaderPage();
        }
    }

    public int getCatalogMetaPageId() {
        return catalogMetaPageId;
    }

    public void setCatalogMetaPageId(int catalogMetaPageId) {
        synchronized (headerLock) {
            ensureOpen();
            this.catalogMetaPageId = catalogMetaPageId;
            writeHeaderPage();
        }
    }

    public long getCheckpointLsn() {
        return checkpointLsn;
    }

    public void setCheckpointLsn(long checkpointLsn) {
        synchronized (headerLock) {
            ensureOpen();
            this.checkpointLsn = checkpointLsn;
            writeHeaderPage();
        }
    }

    public Path getFilePath() {
        return filePath;
    }

    public int allocatePage() {
        synchronized (headerLock) {
            ensureOpen();
            int newPageId = pageCount.getAndIncrement();
            ByteBuffer zeroPage = ByteBuffer.allocate(pageSize);
            writePageInternal(newPageId, zeroPage);
            writeHeaderPage();
            return newPageId;
        }
    }

    public void readPage(int pageId, ByteBuffer target) {
        ensureOpen();
        validatePageId(pageId);
        if (target.remaining() < pageSize) {
            throw new IllegalArgumentException("Target buffer remaining " + target.remaining() + " < pageSize " + pageSize);
        }
        ByteBuffer slice = target.slice().limit(pageSize);
        long offset = (long) pageId * pageSize;
        try {
            readFully(channel, slice, offset);
            target.position(target.position() + pageSize);
        } catch (IOException e) {
            throw new StorageException("Failed to read page " + pageId, e);
        }
    }

    public byte[] readPage(int pageId) {
        byte[] bytes = new byte[pageSize];
        readPage(pageId, ByteBuffer.wrap(bytes));
        return bytes;
    }

    public void writePage(int pageId, ByteBuffer source) {
        ensureOpen();
        validatePageId(pageId);
        if (pageId == 0) {
            throw new IllegalArgumentException("Cannot directly overwrite header page 0 via writePage");
        }
        if (source.remaining() < pageSize) {
            throw new IllegalArgumentException("Source buffer remaining " + source.remaining() + " < pageSize " + pageSize);
        }
        ByteBuffer slice = source.slice().limit(pageSize);
        writePageInternal(pageId, slice);
        source.position(source.position() + pageSize);
    }

    public void writePage(int pageId, byte[] data) {
        if (data.length != pageSize) {
            throw new IllegalArgumentException("Page data length " + data.length + " != pageSize " + pageSize);
        }
        writePage(pageId, ByteBuffer.wrap(data));
    }

    private void writePageInternal(int pageId, ByteBuffer buffer) {
        long offset = (long) pageId * pageSize;
        try {
            writeFully(channel, buffer, offset);
        } catch (IOException e) {
            throw new StorageException("Failed to write page " + pageId, e);
        }
    }

    private void writeHeaderPage() {
        ByteBuffer header = ByteBuffer.allocate(pageSize).order(ByteOrder.BIG_ENDIAN);
        header.putInt(HEADER_MAGIC_OFFSET, MAGIC_NUMBER);
        header.putInt(HEADER_VERSION_OFFSET, FORMAT_VERSION);
        header.putInt(HEADER_PAGE_SIZE_OFFSET, pageSize);
        header.putInt(HEADER_PAGE_COUNT_OFFSET, pageCount.get());
        header.putInt(HEADER_FREE_LIST_HEAD_OFFSET, freeListHead);
        header.putInt(HEADER_CATALOG_PAGE_ID_OFFSET, catalogMetaPageId);
        header.putLong(HEADER_CHECKPOINT_LSN_OFFSET, checkpointLsn);
        writePageInternal(0, header);
    }

    public void fsync() {
        ensureOpen();
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new StorageException("Failed to fsync database file", e);
        }
    }

    private void validatePageId(int pageId) {
        int currentCount = pageCount.get();
        if (pageId < 0 || pageId >= currentCount) {
            throw new IllegalArgumentException("Invalid pageId " + pageId + " (pageCount=" + currentCount + ")");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException("DiskManager is closed");
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer dst, long position) throws IOException {
        long pos = position;
        while (dst.hasRemaining()) {
            int read = channel.read(dst, pos);
            if (read < 0) {
                throw new StorageException("Unexpected EOF while reading at offset " + pos);
            }
            pos += read;
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer src, long position) throws IOException {
        long pos = position;
        while (src.hasRemaining()) {
            int written = channel.write(src, pos);
            pos += written;
        }
    }

    @Override
    public void close() {
        synchronized (headerLock) {
            if (!closed) {
                writeHeaderPage();
                fsync();
                closed = true;
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new StorageException("Failed to close DiskManager channel", e);
                }
            }
        }
    }
}

