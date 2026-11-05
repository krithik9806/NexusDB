package io.nexusdb.core.index;

import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.page.Page;
import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Disk-backed B+Tree index stored in {@link BufferPoolManager} pages.
 *
 * <p>Design & Concurrency/Crash-Safety Invariants:
 * <ul>
 *   <li><b>Stable Root Page ID</b>: {@link #getRootPageId()} remains constant for the lifetime of the
 *       index. When the root page splits, two new child pages are allocated for the left and right halves
 *       and the root page is rewritten in-place as the new internal root, so external catalog references
 *       never become stale across root splits or root collapses.</li>
 *   <li><b>Composite Ordering for Non-Unique Indexes</b>: Unique indexes order entries strictly by
 *       {@link Value} key and reject conflicting {@link RecordId}s with {@link DuplicateKeyException}.
 *       Non-unique indexes order both leaf entries and internal separator keys by the composite pair
 *       {@code (Value key, RecordId recordId)}, guaranteeing $O(\log N)$ point lookup, insertion, and
 *       deletion even when many rows share identical keys across multiple leaf pages.</li>
 *   <li><b>Concurrency Protocol</b>: Tree-level structure mutations and traversals are coordinated by a
 *       {@link ReentrantReadWriteLock} ({@code treeLock}), while individual page accesses acquire per-page
 *       {@link Page#rlatch()}/{@link Page#wlatch()} latches. At most one buffer pool frame is pinned at
 *       any instant during search, insert, and split propagation, preventing buffer pool pin exhaustion.</li>
 * </ul>
 */
public final class BPlusTree {
    public static final int DEFAULT_MAX_KEYS = Integer.MAX_VALUE;
    private static final RecordId MIN_RECORD_ID = new RecordId(0, 0);

    private final BufferPoolManager bufferPoolManager;
    private final int rootPageId;
    private final DataType keyType;
    private final boolean unique;
    private final int maxKeys;
    private final ReentrantReadWriteLock treeLock = new ReentrantReadWriteLock();

    public BPlusTree(BufferPoolManager bufferPoolManager, DataType keyType, boolean unique) {
        this(bufferPoolManager, allocateAndInitRoot(bufferPoolManager, keyType, unique), keyType, unique, DEFAULT_MAX_KEYS);
    }

    public BPlusTree(BufferPoolManager bufferPoolManager, DataType keyType, boolean unique, int maxKeys) {
        this(bufferPoolManager, allocateAndInitRoot(bufferPoolManager, keyType, unique), keyType, unique, maxKeys);
    }

    public BPlusTree(
            BufferPoolManager bufferPoolManager,
            int rootPageId,
            DataType keyType,
            boolean unique) {
        this(bufferPoolManager, rootPageId, keyType, unique, DEFAULT_MAX_KEYS);
    }

    public BPlusTree(
            BufferPoolManager bufferPoolManager,
            int rootPageId,
            DataType keyType,
            boolean unique,
            int maxKeys) {
        this.bufferPoolManager = Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        this.keyType = Objects.requireNonNull(keyType, "keyType must not be null");
        if (rootPageId <= 0) {
            throw new IllegalArgumentException("rootPageId must be > 0, got: " + rootPageId);
        }
        if (maxKeys < 2) {
            throw new IllegalArgumentException("maxKeys must be >= 2, got: " + maxKeys);
        }
        this.rootPageId = rootPageId;
        this.unique = unique;
        this.maxKeys = maxKeys;
    }

    public static BPlusTree create(BufferPoolManager bufferPoolManager, DataType keyType, boolean unique) {
        return new BPlusTree(bufferPoolManager, keyType, unique, DEFAULT_MAX_KEYS);
    }

    public static BPlusTree create(
            BufferPoolManager bufferPoolManager,
            DataType keyType,
            boolean unique,
            int maxKeys) {
        return new BPlusTree(bufferPoolManager, keyType, unique, maxKeys);
    }

    public static BPlusTree open(
            BufferPoolManager bufferPoolManager,
            int rootPageId,
            DataType keyType,
            boolean unique) {
        return new BPlusTree(bufferPoolManager, rootPageId, keyType, unique, DEFAULT_MAX_KEYS);
    }

    public static BPlusTree open(
            BufferPoolManager bufferPoolManager,
            int rootPageId,
            DataType keyType,
            boolean unique,
            int maxKeys) {
        return new BPlusTree(bufferPoolManager, rootPageId, keyType, unique, maxKeys);
    }

    public static BPlusTree open(BufferPoolManager bufferPoolManager, int rootPageId) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Page root = bufferPoolManager.fetchPage(rootPageId);
        DataType detectedKeyType;
        boolean detectedUnique;
        root.rlatch();
        try {
            BPlusTreePage node = BPlusTreePage.wrap(root);
            detectedKeyType = node.getKeyType();
            detectedUnique = node.isUnique();
        } finally {
            root.runlatch();
            bufferPoolManager.unpinPage(rootPageId, false);
        }
        return new BPlusTree(bufferPoolManager, rootPageId, detectedKeyType, detectedUnique, DEFAULT_MAX_KEYS);
    }

    private static int allocateAndInitRoot(
            BufferPoolManager bufferPoolManager,
            DataType keyType,
            boolean unique) {
        Objects.requireNonNull(bufferPoolManager, "bufferPoolManager must not be null");
        Objects.requireNonNull(keyType, "keyType must not be null");
        Page rootPage = bufferPoolManager.newPage();
        int pageId = rootPage.getPageId();
        rootPage.wlatch();
        try {
            BPlusTreePage.initLeaf(rootPage, keyType, unique);
        } finally {
            rootPage.wunlatch();
            bufferPoolManager.unpinPage(pageId, true);
        }
        return pageId;
    }

    public int getRootPageId() {
        return rootPageId;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public boolean isUnique() {
        return unique;
    }

    public int getMaxKeys() {
        return maxKeys;
    }

    public BufferPoolManager getBufferPoolManager() {
        return bufferPoolManager;
    }

    private void validateKey(Value key) {
        Objects.requireNonNull(key, "key must not be null (use Value.NULL for SQL NULL)");
        if (!key.isNull() && key.dataType() != keyType) {
            throw new IllegalArgumentException(
                    "Key type mismatch: index expects " + keyType + ", got " + key.dataType());
        }
        int maxEntryBytes = BPlusTreePage.SLOT_SIZE + BPlusTreePage.internalEntryPayloadBytes(key);
        int maxPayload = bufferPoolManager.getDiskManager().getPageSize() - BPlusTreePage.HEADER_SIZE;
        if (maxEntryBytes * 2 > maxPayload) {
            throw new IllegalArgumentException(
                    "Index key size (" + maxEntryBytes + " bytes) exceeds B+Tree node split capacity");
        }
    }

    /**
     * Point lookup returning all {@link RecordId}s associated with {@code key}.
     */
    public List<RecordId> search(Value key) {
        validateKey(key);
        treeLock.readLock().lock();
        try {
            int leafPageId = findLeafPageId(key, MIN_RECORD_ID);
            List<RecordId> results = new ArrayList<>();

            int currPageId = leafPageId;
            boolean firstPage = true;

            while (currPageId != -1) {
                int nextLeafPageId = -1;
                boolean keepScanning = false;

                Page page = bufferPoolManager.fetchPage(currPageId);
                page.rlatch();
                try {
                    BPlusTreePage leaf = BPlusTreePage.wrap(page);
                    int count = leaf.getKeyCount();
                    int startIdx = 0;
                    if (firstPage) {
                        int bs = leaf.binarySearch(key, MIN_RECORD_ID, !unique);
                        startIdx = bs >= 0 ? bs : (-bs - 1);
                        firstPage = false;
                    }

                    if (startIdx == count) {
                        // Match could start at index 0 of the next leaf if separator fell at boundary
                        nextLeafPageId = leaf.getNextLeafPageId();
                        keepScanning = (nextLeafPageId != -1);
                    } else {
                        for (int i = startIdx; i < count; i++) {
                            int cmp = leaf.compareEntryAt(i, key, 0, 0, false);
                            if (cmp == 0) {
                                results.add(leaf.getRecordId(i));
                                if (unique) {
                                    keepScanning = false;
                                    break;
                                }
                                if (i == count - 1) {
                                    nextLeafPageId = leaf.getNextLeafPageId();
                                    keepScanning = (nextLeafPageId != -1);
                                }
                            } else {
                                keepScanning = false;
                                break;
                            }
                        }
                    }
                } finally {
                    page.runlatch();
                    bufferPoolManager.unpinPage(currPageId, false);
                }

                currPageId = keepScanning ? nextLeafPageId : -1;
            }
            return results;
        } finally {
            treeLock.readLock().unlock();
        }
    }

    private int findLeafPageId(Value key, RecordId probeRecordId) {
        int currPageId = rootPageId;
        while (true) {
            Page page = bufferPoolManager.fetchPage(currPageId);
            page.rlatch();
            int nextPageId;
            boolean isLeaf;
            try {
                BPlusTreePage node = BPlusTreePage.wrap(page);
                isLeaf = node.isLeaf();
                nextPageId = isLeaf ? -1 : node.findChildPageId(key, probeRecordId, !unique);
            } finally {
                page.runlatch();
                bufferPoolManager.unpinPage(currPageId, false);
            }
            if (isLeaf) {
                return currPageId;
            }
            currPageId = nextPageId;
        }
    }

    /**
     * Inserts {@code (key, recordId)} into the B+Tree, splitting overflowing leaf and internal
     * nodes bottom-up as required.
     *
     * @throws DuplicateKeyException if this is a unique index and {@code key} already maps to a different {@link RecordId}
     */
    public void insert(Value key, RecordId recordId) {
        validateKey(key);
        Objects.requireNonNull(recordId, "recordId must not be null");

        treeLock.writeLock().lock();
        try {
            List<Integer> pathPageIds = new ArrayList<>(8);
            int currPageId = rootPageId;

            while (true) {
                pathPageIds.add(currPageId);
                Page page = bufferPoolManager.fetchPage(currPageId);
                page.rlatch();
                boolean isLeaf;
                int nextPageId = -1;
                try {
                    BPlusTreePage node = BPlusTreePage.wrap(page);
                    isLeaf = node.isLeaf();
                    if (!isLeaf) {
                        nextPageId = node.findChildPageId(key, recordId, !unique);
                    }
                } finally {
                    page.runlatch();
                    bufferPoolManager.unpinPage(currPageId, false);
                }
                if (isLeaf) {
                    break;
                }
                currPageId = nextPageId;
            }

            int leafPageId = pathPageIds.get(pathPageIds.size() - 1);
            SplitResult splitResult = insertIntoLeaf(leafPageId, key, recordId);
            if (splitResult == null) {
                return;
            }

            // Propagate split upward along ancestor internal nodes
            for (int level = pathPageIds.size() - 2; level >= 0; level--) {
                int internalPageId = pathPageIds.get(level);
                splitResult = insertIntoInternal(internalPageId, splitResult);
                if (splitResult == null) {
                    return;
                }
            }
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    private record SplitResult(Value promotedKey, RecordId promotedRecordId, int rightPageId) {}

    private SplitResult insertIntoLeaf(int leafPageId, Value key, RecordId recordId) {
        List<IndexEntry> overflowEntries;
        int oldPrevLeaf;
        int oldNextLeaf;

        Page leafPage = bufferPoolManager.fetchPage(leafPageId);
        leafPage.wlatch();
        try {
            BPlusTreePage leaf = BPlusTreePage.wrap(leafPage);
            int bs = leaf.binarySearch(key, recordId, !unique);
            if (bs >= 0) {
                RecordId existingRid = leaf.getRecordId(bs);
                if (unique && !existingRid.equals(recordId)) {
                    throw new DuplicateKeyException(
                            "Duplicate key '" + key + "' in unique index (existing RecordId="
                                    + existingRid + ", new RecordId=" + recordId + ")");
                }
                // Idempotent duplicate of exact (key, recordId)
                leafPage.wunlatch();
                bufferPoolManager.unpinPage(leafPageId, false);
                return null;
            }

            int insertIdx = -bs - 1;
            if (leaf.getKeyCount() < maxKeys && leaf.canFitLeafEntry(key)) {
                leaf.insertLeafAt(insertIdx, key, recordId);
                leafPage.wunlatch();
                bufferPoolManager.unpinPage(leafPageId, true);
                return null;
            }

            overflowEntries = leaf.getAllLeafEntries();
            overflowEntries.add(insertIdx, new IndexEntry(key, recordId));
            oldPrevLeaf = leaf.getPrevLeafPageId();
            oldNextLeaf = leaf.getNextLeafPageId();
        } catch (RuntimeException e) {
            leafPage.wunlatch();
            bufferPoolManager.unpinPage(leafPageId, false);
            throw e;
        }
        leafPage.wunlatch();
        bufferPoolManager.unpinPage(leafPageId, false);

        int splitIdx = overflowEntries.size() / 2;
        List<IndexEntry> leftEntries = overflowEntries.subList(0, splitIdx);
        List<IndexEntry> rightEntries = overflowEntries.subList(splitIdx, overflowEntries.size());
        IndexEntry promoted = rightEntries.get(0);

        if (leafPageId == rootPageId) {
            // Root leaf split: allocate two child leaf pages and convert rootPageId into an internal root
            Page leftChildRaw = bufferPoolManager.newPage();
            int leftChildPageId = leftChildRaw.getPageId();
            leftChildRaw.wlatch();
            try {
                BPlusTreePage leftLeaf = BPlusTreePage.initLeaf(leftChildRaw, keyType, unique);
                leftLeaf.setAllLeafEntries(leftEntries);
                leftLeaf.setPrevLeafPageId(-1);
                leftLeaf.setNextLeafPageId(-1);
            } finally {
                leftChildRaw.wunlatch();
                bufferPoolManager.unpinPage(leftChildPageId, true);
            }

            Page rightChildRaw = bufferPoolManager.newPage();
            int rightChildPageId = rightChildRaw.getPageId();
            rightChildRaw.wlatch();
            try {
                BPlusTreePage rightLeaf = BPlusTreePage.initLeaf(rightChildRaw, keyType, unique);
                rightLeaf.setAllLeafEntries(rightEntries);
                rightLeaf.setPrevLeafPageId(leftChildPageId);
                rightLeaf.setNextLeafPageId(-1);
            } finally {
                rightChildRaw.wunlatch();
                bufferPoolManager.unpinPage(rightChildPageId, true);
            }

            Page leftUpdate = bufferPoolManager.fetchPage(leftChildPageId);
            leftUpdate.wlatch();
            try {
                BPlusTreePage.wrap(leftUpdate).setNextLeafPageId(rightChildPageId);
            } finally {
                leftUpdate.wunlatch();
                bufferPoolManager.unpinPage(leftChildPageId, true);
            }

            Page rootRaw = bufferPoolManager.fetchPage(rootPageId);
            rootRaw.wlatch();
            try {
                BPlusTreePage rootInternal = BPlusTreePage.initInternal(rootRaw, keyType, unique, leftChildPageId);
                rootInternal.insertInternalAt(0, promoted.key(), promoted.recordId(), rightChildPageId);
            } finally {
                rootRaw.wunlatch();
                bufferPoolManager.unpinPage(rootPageId, true);
            }
            return null;
        }

        // Non-root leaf split: allocate right sibling leaf and link leaf sibling pointers
        Page rightRaw = bufferPoolManager.newPage();
        int rightLeafPageId = rightRaw.getPageId();
        rightRaw.wlatch();
        try {
            BPlusTreePage rightLeaf = BPlusTreePage.initLeaf(rightRaw, keyType, unique);
            rightLeaf.setAllLeafEntries(rightEntries);
            rightLeaf.setPrevLeafPageId(leafPageId);
            rightLeaf.setNextLeafPageId(oldNextLeaf);
        } finally {
            rightRaw.wunlatch();
            bufferPoolManager.unpinPage(rightLeafPageId, true);
        }

        Page leftUpdate = bufferPoolManager.fetchPage(leafPageId);
        leftUpdate.wlatch();
        try {
            BPlusTreePage leftLeaf = BPlusTreePage.wrap(leftUpdate);
            leftLeaf.setAllLeafEntries(leftEntries);
            leftLeaf.setPrevLeafPageId(oldPrevLeaf);
            leftLeaf.setNextLeafPageId(rightLeafPageId);
        } finally {
            leftUpdate.wunlatch();
            bufferPoolManager.unpinPage(leafPageId, true);
        }

        if (oldNextLeaf != -1) {
            Page nextLeafRaw = bufferPoolManager.fetchPage(oldNextLeaf);
            nextLeafRaw.wlatch();
            try {
                BPlusTreePage.wrap(nextLeafRaw).setPrevLeafPageId(rightLeafPageId);
            } finally {
                nextLeafRaw.wunlatch();
                bufferPoolManager.unpinPage(oldNextLeaf, true);
            }
        }

        return new SplitResult(promoted.key(), promoted.recordId(), rightLeafPageId);
    }

    private SplitResult insertIntoInternal(int internalPageId, SplitResult childSplit) {
        Value key = childSplit.promotedKey();
        RecordId recordId = childSplit.promotedRecordId();
        int rightChildPageId = childSplit.rightPageId();

        int firstChildPageId;
        List<BPlusTreePage.InternalEntry> overflowEntries;

        Page internalPage = bufferPoolManager.fetchPage(internalPageId);
        internalPage.wlatch();
        try {
            BPlusTreePage internal = BPlusTreePage.wrap(internalPage);
            int bs = internal.binarySearch(key, recordId, !unique);
            int insertIdx = bs >= 0 ? (bs + 1) : (-bs - 1);

            if (internal.getKeyCount() < maxKeys && internal.canFitInternalEntry(key)) {
                internal.insertInternalAt(insertIdx, key, recordId, rightChildPageId);
                internalPage.wunlatch();
                bufferPoolManager.unpinPage(internalPageId, true);
                return null;
            }

            firstChildPageId = internal.getFirstChildPageId();
            overflowEntries = internal.getAllInternalEntries();
            overflowEntries.add(insertIdx, new BPlusTreePage.InternalEntry(key, recordId, rightChildPageId));
        } catch (RuntimeException e) {
            internalPage.wunlatch();
            bufferPoolManager.unpinPage(internalPageId, false);
            throw e;
        }
        internalPage.wunlatch();
        bufferPoolManager.unpinPage(internalPageId, false);

        int midIdx = overflowEntries.size() / 2;
        BPlusTreePage.InternalEntry pushedUp = overflowEntries.get(midIdx);
        List<BPlusTreePage.InternalEntry> leftEntries = overflowEntries.subList(0, midIdx);
        List<BPlusTreePage.InternalEntry> rightEntries = overflowEntries.subList(midIdx + 1, overflowEntries.size());
        int rightFirstChildPageId = pushedUp.rightChildPageId();

        if (internalPageId == rootPageId) {
            // Internal root split: allocate left and right internal children and rewrite rootPageId
            Page leftRaw = bufferPoolManager.newPage();
            int leftInternalPageId = leftRaw.getPageId();
            leftRaw.wlatch();
            try {
                BPlusTreePage leftNode = BPlusTreePage.initInternal(leftRaw, keyType, unique, firstChildPageId);
                leftNode.setAllInternalEntries(firstChildPageId, leftEntries);
            } finally {
                leftRaw.wunlatch();
                bufferPoolManager.unpinPage(leftInternalPageId, true);
            }

            Page rightRaw = bufferPoolManager.newPage();
            int rightInternalPageId = rightRaw.getPageId();
            rightRaw.wlatch();
            try {
                BPlusTreePage rightNode = BPlusTreePage.initInternal(rightRaw, keyType, unique, rightFirstChildPageId);
                rightNode.setAllInternalEntries(rightFirstChildPageId, rightEntries);
            } finally {
                rightRaw.wunlatch();
                bufferPoolManager.unpinPage(rightInternalPageId, true);
            }

            Page rootRaw = bufferPoolManager.fetchPage(rootPageId);
            rootRaw.wlatch();
            try {
                BPlusTreePage rootNode = BPlusTreePage.initInternal(rootRaw, keyType, unique, leftInternalPageId);
                rootNode.insertInternalAt(0, pushedUp.key(), pushedUp.recordId(), rightInternalPageId);
            } finally {
                rootRaw.wunlatch();
                bufferPoolManager.unpinPage(rootPageId, true);
            }
            return null;
        }

        // Non-root internal split
        Page leftUpdate = bufferPoolManager.fetchPage(internalPageId);
        leftUpdate.wlatch();
        try {
            BPlusTreePage leftNode = BPlusTreePage.wrap(leftUpdate);
            leftNode.setAllInternalEntries(firstChildPageId, leftEntries);
        } finally {
            leftUpdate.wunlatch();
            bufferPoolManager.unpinPage(internalPageId, true);
        }

        Page rightRaw = bufferPoolManager.newPage();
        int rightInternalPageId = rightRaw.getPageId();
        rightRaw.wlatch();
        try {
            BPlusTreePage rightNode = BPlusTreePage.initInternal(rightRaw, keyType, unique, rightFirstChildPageId);
            rightNode.setAllInternalEntries(rightFirstChildPageId, rightEntries);
        } finally {
            rightRaw.wunlatch();
            bufferPoolManager.unpinPage(rightInternalPageId, true);
        }

        return new SplitResult(pushedUp.key(), pushedUp.recordId(), rightInternalPageId);
    }

    /**
     * Returns the current height of the B+Tree (1 for a single root leaf page).
     */
    public int getHeight() {
        treeLock.readLock().lock();
        try {
            int height = 1;
            int currPageId = rootPageId;
            while (true) {
                Page page = bufferPoolManager.fetchPage(currPageId);
                page.rlatch();
                boolean isLeaf;
                int firstChild;
                try {
                    BPlusTreePage node = BPlusTreePage.wrap(page);
                    isLeaf = node.isLeaf();
                    firstChild = node.getFirstChildPageId();
                } finally {
                    page.runlatch();
                    bufferPoolManager.unpinPage(currPageId, false);
                }
                if (isLeaf) {
                    return height;
                }
                height++;
                currPageId = firstChild;
            }
        } finally {
            treeLock.readLock().unlock();
        }
    }

    /**
     * Verifies all structural and key-ordering invariants of the B+Tree:
     * <ol>
     *   <li>Strict key ordering within every internal and leaf node</li>
     *   <li>Parent separator lower/upper boundaries across all descendants</li>
     *   <li>Uniform balanced depth across all leaf nodes</li>
     *   <li>Doubly-linked leaf sibling chain continuity and cross-leaf key monotonicity</li>
     * </ol>
     *
     * @throws IllegalStateException if any B+Tree invariant is violated
     */
    public void verifyInvariants() {
        treeLock.readLock().lock();
        try {
            Set<Integer> visitedPages = new HashSet<>();
            List<LeafSummary> leavesInTreeOrder = new ArrayList<>();
            int[] expectedLeafDepth = {-1};

            verifySubtree(rootPageId, 0, null, null, visitedPages, leavesInTreeOrder, expectedLeafDepth);

            if (leavesInTreeOrder.isEmpty()) {
                throw new IllegalStateException("B+Tree has no leaf nodes");
            }

            for (int i = 0; i < leavesInTreeOrder.size(); i++) {
                LeafSummary curr = leavesInTreeOrder.get(i);
                int expectedPrev = (i == 0) ? -1 : leavesInTreeOrder.get(i - 1).pageId();
                int expectedNext = (i == leavesInTreeOrder.size() - 1) ? -1 : leavesInTreeOrder.get(i + 1).pageId();

                if (curr.prevLeafPageId() != expectedPrev) {
                    throw new IllegalStateException(
                            "Leaf " + curr.pageId() + " has prevLeafPageId=" + curr.prevLeafPageId()
                                    + ", expected " + expectedPrev);
                }
                if (curr.nextLeafPageId() != expectedNext) {
                    throw new IllegalStateException(
                            "Leaf " + curr.pageId() + " has nextLeafPageId=" + curr.nextLeafPageId()
                                    + ", expected " + expectedNext);
                }
                if (i > 0) {
                    LeafSummary prev = leavesInTreeOrder.get(i - 1);
                    if (prev.lastEntry() != null && curr.firstEntry() != null) {
                        int cmp = IndexEntry.compare(
                                prev.lastEntry().key(),
                                prev.lastEntry().recordId(),
                                curr.firstEntry().key(),
                                curr.firstEntry().recordId(),
                                unique);
                        if (cmp >= 0) {
                            throw new IllegalStateException(
                                    "Cross-leaf key ordering violation between leaf " + prev.pageId()
                                            + " (" + prev.lastEntry() + ") and leaf " + curr.pageId()
                                            + " (" + curr.firstEntry() + ")");
                        }
                    }
                }
            }
        } finally {
            treeLock.readLock().unlock();
        }
    }

    private record LeafSummary(
            int pageId,
            int prevLeafPageId,
            int nextLeafPageId,
            IndexEntry firstEntry,
            IndexEntry lastEntry) {}

    private void verifySubtree(
            int pageId,
            int depth,
            IndexEntry lowerBound,
            IndexEntry upperBound,
            Set<Integer> visitedPages,
            List<LeafSummary> leavesInTreeOrder,
            int[] expectedLeafDepth) {
        if (!visitedPages.add(pageId)) {
            throw new IllegalStateException("Cycle or shared page detected in B+Tree at pageId=" + pageId);
        }

        boolean isLeaf;
        int keyCount;
        int prevLeaf;
        int nextLeaf;
        int firstChild;
        List<IndexEntry> leafEntries = null;
        List<BPlusTreePage.InternalEntry> internalEntries = null;

        Page page = bufferPoolManager.fetchPage(pageId);
        page.rlatch();
        try {
            BPlusTreePage node = BPlusTreePage.wrap(page);
            if (node.getKeyType() != keyType) {
                throw new IllegalStateException(
                        "Page " + pageId + " keyType=" + node.getKeyType() + " != tree keyType=" + keyType);
            }
            if (node.isUnique() != unique) {
                throw new IllegalStateException(
                        "Page " + pageId + " unique=" + node.isUnique() + " != tree unique=" + unique);
            }
            isLeaf = node.isLeaf();
            keyCount = node.getKeyCount();
            prevLeaf = node.getPrevLeafPageId();
            nextLeaf = node.getNextLeafPageId();
            firstChild = node.getFirstChildPageId();
            if (isLeaf) {
                leafEntries = node.getAllLeafEntries();
            } else if (node.isInternal()) {
                internalEntries = node.getAllInternalEntries();
            } else {
                throw new IllegalStateException("Unknown BPlusTreePage nodeType=" + node.getNodeType());
            }
        } finally {
            page.runlatch();
            bufferPoolManager.unpinPage(pageId, false);
        }

        if (keyCount > maxKeys) {
            throw new IllegalStateException(
                    "Node " + pageId + " exceeds maxKeys (" + keyCount + " > " + maxKeys + ")");
        }

        if (isLeaf) {
            if (pageId != rootPageId && keyCount == 0) {
                throw new IllegalStateException("Non-root leaf page " + pageId + " is empty");
            }
            if (expectedLeafDepth[0] == -1) {
                expectedLeafDepth[0] = depth;
            } else if (expectedLeafDepth[0] != depth) {
                throw new IllegalStateException(
                        "Unbalanced B+Tree leaf depth at page " + pageId
                                + ": got " + depth + ", expected " + expectedLeafDepth[0]);
            }

            for (int i = 0; i < leafEntries.size(); i++) {
                IndexEntry curr = leafEntries.get(i);
                if (i > 0) {
                    IndexEntry prev = leafEntries.get(i - 1);
                    int cmp = IndexEntry.compare(
                            prev.key(), prev.recordId(), curr.key(), curr.recordId(), unique);
                    if (cmp >= 0) {
                        throw new IllegalStateException(
                                "Key ordering violation in leaf " + pageId + " at index " + i
                                        + ": " + prev + " >= " + curr);
                    }
                }
                checkBounds(pageId, curr.key(), curr.recordId(), lowerBound, upperBound);
            }

            IndexEntry first = leafEntries.isEmpty() ? null : leafEntries.get(0);
            IndexEntry last = leafEntries.isEmpty() ? null : leafEntries.get(leafEntries.size() - 1);
            leavesInTreeOrder.add(new LeafSummary(pageId, prevLeaf, nextLeaf, first, last));
            return;
        }

        // Internal node checks
        if (keyCount == 0) {
            throw new IllegalStateException("Internal node " + pageId + " has 0 keys");
        }
        if (firstChild <= 0) {
            throw new IllegalStateException("Internal node " + pageId + " has invalid firstChildPageId=" + firstChild);
        }

        for (int i = 0; i < internalEntries.size(); i++) {
            BPlusTreePage.InternalEntry curr = internalEntries.get(i);
            if (curr.rightChildPageId() <= 0) {
                throw new IllegalStateException(
                        "Internal node " + pageId + " has invalid rightChildPageId at index " + i);
            }
            if (i > 0) {
                BPlusTreePage.InternalEntry prev = internalEntries.get(i - 1);
                int cmp = IndexEntry.compare(
                        prev.key(), prev.recordId(), curr.key(), curr.recordId(), unique);
                if (cmp >= 0) {
                    throw new IllegalStateException(
                            "Separator ordering violation in internal node " + pageId + " at index " + i);
                }
            }
            checkBounds(pageId, curr.key(), curr.recordId(), lowerBound, upperBound);
        }

        // Verify children recursively with tightened [lowerBound, upperBound) intervals
        IndexEntry firstSep = new IndexEntry(internalEntries.get(0).key(), internalEntries.get(0).recordId());
        verifySubtree(firstChild, depth + 1, lowerBound, firstSep, visitedPages, leavesInTreeOrder, expectedLeafDepth);

        for (int i = 0; i < internalEntries.size(); i++) {
            BPlusTreePage.InternalEntry entry = internalEntries.get(i);
            IndexEntry childLow = new IndexEntry(entry.key(), entry.recordId());
            IndexEntry childHigh = (i + 1 < internalEntries.size())
                    ? new IndexEntry(internalEntries.get(i + 1).key(), internalEntries.get(i + 1).recordId())
                    : upperBound;
            verifySubtree(
                    entry.rightChildPageId(),
                    depth + 1,
                    childLow,
                    childHigh,
                    visitedPages,
                    leavesInTreeOrder,
                    expectedLeafDepth);
        }
    }

    private void checkBounds(
            int pageId,
            Value key,
            RecordId recordId,
            IndexEntry lowerBound,
            IndexEntry upperBound) {
        if (lowerBound != null) {
            int cmpLow = IndexEntry.compare(key, recordId, lowerBound.key(), lowerBound.recordId(), unique);
            if (cmpLow < 0) {
                throw new IllegalStateException(
                        "Entry (" + key + ", " + recordId + ") on page " + pageId
                                + " violates lower separator bound " + lowerBound);
            }
        }
        if (upperBound != null) {
            int cmpHigh = IndexEntry.compare(key, recordId, upperBound.key(), upperBound.recordId(), unique);
            if (cmpHigh >= 0) {
                throw new IllegalStateException(
                        "Entry (" + key + ", " + recordId + ") on page " + pageId
                                + " violates upper separator bound " + upperBound);
            }
        }
    }
}
