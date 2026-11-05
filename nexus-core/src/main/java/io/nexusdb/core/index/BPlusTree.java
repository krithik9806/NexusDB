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
 *       and the root page is rewritten in-place as the new internal root; when an internal root loses its
 *       last separator key during deletion, its single remaining child is copied into {@code rootPageId}
 *       and deallocated. External catalog references therefore never become stale across splits or shrinks.</li>
 *   <li><b>Composite Ordering for Non-Unique Indexes</b>: Unique indexes order entries strictly by
 *       {@link Value} key and reject conflicting {@link RecordId}s with {@link DuplicateKeyException}.
 *       Non-unique indexes order both leaf entries and internal separator keys by the composite pair
 *       {@code (Value key, RecordId recordId)}, guaranteeing $O(\log N)$ point lookup, insertion, and
 *       deletion even when many rows share identical keys across multiple leaf pages.</li>
 *   <li><b>Concurrency Protocol</b>: Tree-level structure mutations and traversals are coordinated by a
 *       {@link ReentrantReadWriteLock} ({@code treeLock}), while individual page accesses acquire per-page
 *       {@link Page#rlatch()}/{@link Page#wlatch()} latches. At most one buffer pool frame is pinned at
 *       any instant during search, range scan, insert, and delete/merge propagation.</li>
 * </ul>
 */
public final class BPlusTree {
    public static final int DEFAULT_MAX_KEYS = Integer.MAX_VALUE;
    private static final RecordId MIN_RECORD_ID = new RecordId(0, 0);
    private static final RecordId MAX_RECORD_ID = new RecordId(Integer.MAX_VALUE, Integer.MAX_VALUE);

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

    /**
     * Returns a forward {@link BPlusTreeRangeIterator} over entries matching the specified range bounds.
     * Passing {@code null} for {@code lowKey} or {@code highKey} leaves that side of the range unbounded.
     */
    public BPlusTreeRangeIterator rangeScan(
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive) {
        if (lowKey != null) {
            validateKey(lowKey);
        }
        if (highKey != null) {
            validateKey(highKey);
        }
        if (lowKey != null && highKey != null) {
            int boundCmp = lowKey.compareTo(highKey);
            if (boundCmp > 0 || (boundCmp == 0 && (!lowInclusive || !highInclusive))) {
                return new BPlusTreeRangeIterator(this, -1, lowKey, lowInclusive, highKey, highInclusive);
            }
        }

        int startLeafPageId;
        treeLock.readLock().lock();
        try {
            if (lowKey == null) {
                startLeafPageId = findLeftmostLeafPageId();
            } else {
                RecordId probeRid = lowInclusive ? MIN_RECORD_ID : MAX_RECORD_ID;
                startLeafPageId = findLeafPageId(lowKey, probeRid);
            }
        } finally {
            treeLock.readLock().unlock();
        }
        return new BPlusTreeRangeIterator(this, startLeafPageId, lowKey, lowInclusive, highKey, highInclusive);
    }

    int readRangeLeafBatch(
            int leafPageId,
            boolean firstLeaf,
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive,
            List<IndexEntry> outBatch) {
        treeLock.readLock().lock();
        try {
            Page page = bufferPoolManager.fetchPage(leafPageId);
            page.rlatch();
            try {
                BPlusTreePage leaf = BPlusTreePage.wrap(page);
                if (!leaf.isLeaf()) {
                    return -1;
                }
                int count = leaf.getKeyCount();
                int startIdx = 0;

                if (firstLeaf && lowKey != null) {
                    RecordId probeRid = lowInclusive ? MIN_RECORD_ID : MAX_RECORD_ID;
                    int bs = leaf.binarySearch(lowKey, probeRid, !unique);
                    if (bs >= 0) {
                        startIdx = lowInclusive ? bs : (bs + 1);
                    } else {
                        startIdx = -bs - 1;
                    }
                }

                for (int i = startIdx; i < count; i++) {
                    if (lowKey != null) {
                        int lowCmp = leaf.compareEntryAt(i, lowKey, 0, 0, false);
                        if (lowCmp < 0 || (lowCmp == 0 && !lowInclusive)) {
                            continue;
                        }
                    }
                    if (highKey != null) {
                        int highCmp = leaf.compareEntryAt(i, highKey, 0, 0, false);
                        if (highCmp > 0 || (highCmp == 0 && !highInclusive)) {
                            return -1;
                        }
                    }
                    outBatch.add(leaf.getLeafEntry(i));
                }
                return leaf.getNextLeafPageId();
            } finally {
                page.runlatch();
                bufferPoolManager.unpinPage(leafPageId, false);
            }
        } finally {
            treeLock.readLock().unlock();
        }
    }

    private int findLeftmostLeafPageId() {
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
                return currPageId;
            }
            currPageId = firstChild;
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

    /**
     * Deletes all entries associated with {@code key} from the B+Tree.
     *
     * @return the number of removed entries
     */
    public int deleteKey(Value key) {
        validateKey(key);
        List<RecordId> rids = search(key);
        int removed = 0;
        for (RecordId rid : rids) {
            if (delete(key, rid)) {
                removed++;
            }
        }
        return removed;
    }

    /**
     * Deletes the specific {@code (key, recordId)} entry from the B+Tree, handling node underflow
     * via left/right sibling redistribution (borrow) or sibling merge and shrinking the root when
     * an internal root is reduced to a single child.
     *
     * @return {@code true} if the entry existed and was deleted, {@code false} otherwise
     */
    public boolean delete(Value key, RecordId recordId) {
        validateKey(key);
        Objects.requireNonNull(recordId, "recordId must not be null");

        treeLock.writeLock().lock();
        try {
            List<Integer> pathPageIds = new ArrayList<>(8);
            List<Integer> pathChildIndices = new ArrayList<>(8);
            int currPageId = rootPageId;

            while (true) {
                pathPageIds.add(currPageId);
                Page page = bufferPoolManager.fetchPage(currPageId);
                page.rlatch();
                boolean isLeaf;
                int childIdx = -1;
                int nextPageId = -1;
                try {
                    BPlusTreePage node = BPlusTreePage.wrap(page);
                    isLeaf = node.isLeaf();
                    if (!isLeaf) {
                        childIdx = node.findChildIndex(key, recordId, !unique);
                        nextPageId = node.getChildPageId(childIdx);
                    }
                } finally {
                    page.runlatch();
                    bufferPoolManager.unpinPage(currPageId, false);
                }
                if (isLeaf) {
                    break;
                }
                pathChildIndices.add(childIdx);
                currPageId = nextPageId;
            }

            int leafPageId = pathPageIds.get(pathPageIds.size() - 1);
            boolean leafUnderflow;

            Page leafPage = bufferPoolManager.fetchPage(leafPageId);
            leafPage.wlatch();
            try {
                BPlusTreePage leaf = BPlusTreePage.wrap(leafPage);
                int bs = leaf.binarySearch(key, recordId, !unique);
                if (bs < 0 || !leaf.getRecordId(bs).equals(recordId)) {
                    leafPage.wunlatch();
                    bufferPoolManager.unpinPage(leafPageId, false);
                    return false;
                }
                leaf.removeAt(bs);
                leafUnderflow = (leafPageId != rootPageId) && isNodeUnderflow(leaf);
            } catch (RuntimeException e) {
                leafPage.wunlatch();
                bufferPoolManager.unpinPage(leafPageId, false);
                throw e;
            }
            leafPage.wunlatch();
            bufferPoolManager.unpinPage(leafPageId, true);

            if (!leafUnderflow) {
                return true;
            }

            for (int level = pathPageIds.size() - 1; level >= 1; level--) {
                int childPageId = pathPageIds.get(level);
                int parentPageId = pathPageIds.get(level - 1);
                int childIdx = pathChildIndices.get(level - 1);
                boolean parentUnderflow = rebalanceUnderflowChild(parentPageId, childIdx, childPageId);
                if (!parentUnderflow) {
                    break;
                }
            }
            return true;
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    private boolean isNodeUnderflow(BPlusTreePage node) {
        int count = node.getKeyCount();
        if (count == 0) {
            return true;
        }
        if (maxKeys < DEFAULT_MAX_KEYS) {
            int minKeys = Math.max(1, maxKeys / 2);
            return count < minKeys;
        }
        int minBytes = node.getMaxPayloadBytes() / 3;
        return node.getUsedBytes() < minBytes;
    }

    private boolean canSiblingLend(BPlusTreePage sibling, int removedEntryTotalBytes) {
        int count = sibling.getKeyCount();
        if (count <= 1) {
            return false;
        }
        if (maxKeys < DEFAULT_MAX_KEYS) {
            int minKeys = Math.max(1, maxKeys / 2);
            return (count - 1) >= minKeys;
        }
        int minBytes = sibling.getMaxPayloadBytes() / 3;
        return (sibling.getUsedBytes() - removedEntryTotalBytes) >= minBytes;
    }

    private boolean rebalanceUnderflowChild(int parentPageId, int childIdx, int childPageId) {
        boolean childIsLeaf;
        Page childProbe = bufferPoolManager.fetchPage(childPageId);
        childProbe.rlatch();
        try {
            BPlusTreePage childNode = BPlusTreePage.wrap(childProbe);
            if (!isNodeUnderflow(childNode)) {
                return false;
            }
            childIsLeaf = childNode.isLeaf();
        } finally {
            childProbe.runlatch();
            bufferPoolManager.unpinPage(childPageId, false);
        }

        int parentKeyCount;
        int leftSiblingPageId = -1;
        int rightSiblingPageId = -1;

        Page parentProbe = bufferPoolManager.fetchPage(parentPageId);
        parentProbe.rlatch();
        try {
            BPlusTreePage parentNode = BPlusTreePage.wrap(parentProbe);
            parentKeyCount = parentNode.getKeyCount();
            if (childIdx > 0) {
                leftSiblingPageId = parentNode.getChildPageId(childIdx - 1);
            }
            if (childIdx < parentKeyCount) {
                rightSiblingPageId = parentNode.getChildPageId(childIdx + 1);
            }
        } finally {
            parentProbe.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        if (childIsLeaf) {
            if (leftSiblingPageId != -1 && tryBorrowLeafFromLeft(parentPageId, childIdx - 1, leftSiblingPageId, childPageId)) {
                return false;
            }
            if (rightSiblingPageId != -1 && tryBorrowLeafFromRight(parentPageId, childIdx, childPageId, rightSiblingPageId)) {
                return false;
            }
            if (leftSiblingPageId != -1 && canMergeLeaf(leftSiblingPageId, childPageId)) {
                return mergeLeafNodes(parentPageId, childIdx - 1, leftSiblingPageId, childPageId);
            }
            if (rightSiblingPageId != -1 && canMergeLeaf(childPageId, rightSiblingPageId)) {
                return mergeLeafNodes(parentPageId, childIdx, childPageId, rightSiblingPageId);
            }
            return false;
        } else {
            if (leftSiblingPageId != -1 && tryBorrowInternalFromLeft(parentPageId, childIdx - 1, leftSiblingPageId, childPageId)) {
                return false;
            }
            if (rightSiblingPageId != -1 && tryBorrowInternalFromRight(parentPageId, childIdx, childPageId, rightSiblingPageId)) {
                return false;
            }
            if (leftSiblingPageId != -1 && canMergeInternal(parentPageId, childIdx - 1, leftSiblingPageId, childPageId)) {
                return mergeInternalNodes(parentPageId, childIdx - 1, leftSiblingPageId, childPageId);
            }
            if (rightSiblingPageId != -1 && canMergeInternal(parentPageId, childIdx, childPageId, rightSiblingPageId)) {
                return mergeInternalNodes(parentPageId, childIdx, childPageId, rightSiblingPageId);
            }
            return false;
        }
    }

    private boolean canMergeLeaf(int leftPageId, int rightPageId) {
        int leftCount;
        int leftUsed;
        int maxPayload;
        Page leftPage = bufferPoolManager.fetchPage(leftPageId);
        leftPage.rlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftPage);
            leftCount = left.getKeyCount();
            leftUsed = left.getUsedBytes();
            maxPayload = left.getMaxPayloadBytes();
        } finally {
            leftPage.runlatch();
            bufferPoolManager.unpinPage(leftPageId, false);
        }

        int rightCount;
        int rightUsed;
        Page rightPage = bufferPoolManager.fetchPage(rightPageId);
        rightPage.rlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightPage);
            rightCount = right.getKeyCount();
            rightUsed = right.getUsedBytes();
        } finally {
            rightPage.runlatch();
            bufferPoolManager.unpinPage(rightPageId, false);
        }

        return (long) leftCount + rightCount <= maxKeys && (leftUsed + rightUsed) <= maxPayload;
    }

    private boolean canMergeInternal(int parentPageId, int sepIdx, int leftPageId, int rightPageId) {
        int sepBytes;
        Page parentPage = bufferPoolManager.fetchPage(parentPageId);
        parentPage.rlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentPage);
            Value sepKey = parent.getKey(sepIdx);
            sepBytes = BPlusTreePage.SLOT_SIZE + BPlusTreePage.internalEntryPayloadBytes(sepKey);
        } finally {
            parentPage.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        int leftCount;
        int leftUsed;
        int maxPayload;
        Page leftPage = bufferPoolManager.fetchPage(leftPageId);
        leftPage.rlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftPage);
            leftCount = left.getKeyCount();
            leftUsed = left.getUsedBytes();
            maxPayload = left.getMaxPayloadBytes();
        } finally {
            leftPage.runlatch();
            bufferPoolManager.unpinPage(leftPageId, false);
        }

        int rightCount;
        int rightUsed;
        Page rightPage = bufferPoolManager.fetchPage(rightPageId);
        rightPage.rlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightPage);
            rightCount = right.getKeyCount();
            rightUsed = right.getUsedBytes();
        } finally {
            rightPage.runlatch();
            bufferPoolManager.unpinPage(rightPageId, false);
        }

        return (long) leftCount + 1 + rightCount <= maxKeys && (leftUsed + sepBytes + rightUsed) <= maxPayload;
    }

    private boolean tryBorrowLeafFromLeft(int parentPageId, int sepIdx, int leftPageId, int childPageId) {
        IndexEntry candidate;
        Page leftPage = bufferPoolManager.fetchPage(leftPageId);
        leftPage.rlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftPage);
            int count = left.getKeyCount();
            if (count <= 1) {
                return false;
            }
            candidate = left.getLeafEntry(count - 1);
            int entryBytes = BPlusTreePage.SLOT_SIZE + BPlusTreePage.leafEntryPayloadBytes(candidate.key());
            if (!canSiblingLend(left, entryBytes)) {
                return false;
            }
        } finally {
            leftPage.runlatch();
            bufferPoolManager.unpinPage(leftPageId, false);
        }

        Page childPage = bufferPoolManager.fetchPage(childPageId);
        childPage.rlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childPage);
            if (child.getKeyCount() >= maxKeys || !child.canFitLeafEntry(candidate.key())) {
                return false;
            }
        } finally {
            childPage.runlatch();
            bufferPoolManager.unpinPage(childPageId, false);
        }

        Page parentPage = bufferPoolManager.fetchPage(parentPageId);
        parentPage.rlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentPage);
            if (!parent.canReplaceInternalKeyAt(sepIdx, candidate.key())) {
                return false;
            }
        } finally {
            parentPage.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        // Execute leaf borrow from left sibling
        Page leftMut = bufferPoolManager.fetchPage(leftPageId);
        leftMut.wlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftMut);
            left.removeAt(left.getKeyCount() - 1);
        } finally {
            leftMut.wunlatch();
            bufferPoolManager.unpinPage(leftPageId, true);
        }

        Page childMut = bufferPoolManager.fetchPage(childPageId);
        childMut.wlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childMut);
            child.insertLeafAt(0, candidate.key(), candidate.recordId());
        } finally {
            childMut.wunlatch();
            bufferPoolManager.unpinPage(childPageId, true);
        }

        Page parentMut = bufferPoolManager.fetchPage(parentPageId);
        parentMut.wlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentMut);
            parent.setInternalKeyAt(sepIdx, candidate.key(), candidate.recordId());
        } finally {
            parentMut.wunlatch();
            bufferPoolManager.unpinPage(parentPageId, true);
        }
        return true;
    }

    private boolean tryBorrowLeafFromRight(int parentPageId, int sepIdx, int childPageId, int rightPageId) {
        IndexEntry candidate;
        IndexEntry newRightMin;
        Page rightPage = bufferPoolManager.fetchPage(rightPageId);
        rightPage.rlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightPage);
            int count = right.getKeyCount();
            if (count <= 1) {
                return false;
            }
            candidate = right.getLeafEntry(0);
            newRightMin = right.getLeafEntry(1);
            int entryBytes = BPlusTreePage.SLOT_SIZE + BPlusTreePage.leafEntryPayloadBytes(candidate.key());
            if (!canSiblingLend(right, entryBytes)) {
                return false;
            }
        } finally {
            rightPage.runlatch();
            bufferPoolManager.unpinPage(rightPageId, false);
        }

        Page childPage = bufferPoolManager.fetchPage(childPageId);
        childPage.rlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childPage);
            if (child.getKeyCount() >= maxKeys || !child.canFitLeafEntry(candidate.key())) {
                return false;
            }
        } finally {
            childPage.runlatch();
            bufferPoolManager.unpinPage(childPageId, false);
        }

        Page parentPage = bufferPoolManager.fetchPage(parentPageId);
        parentPage.rlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentPage);
            if (!parent.canReplaceInternalKeyAt(sepIdx, newRightMin.key())) {
                return false;
            }
        } finally {
            parentPage.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        // Execute leaf borrow from right sibling
        Page rightMut = bufferPoolManager.fetchPage(rightPageId);
        rightMut.wlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightMut);
            right.removeAt(0);
        } finally {
            rightMut.wunlatch();
            bufferPoolManager.unpinPage(rightPageId, true);
        }

        Page childMut = bufferPoolManager.fetchPage(childPageId);
        childMut.wlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childMut);
            child.insertLeafAt(child.getKeyCount(), candidate.key(), candidate.recordId());
        } finally {
            childMut.wunlatch();
            bufferPoolManager.unpinPage(childPageId, true);
        }

        Page parentMut = bufferPoolManager.fetchPage(parentPageId);
        parentMut.wlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentMut);
            parent.setInternalKeyAt(sepIdx, newRightMin.key(), newRightMin.recordId());
        } finally {
            parentMut.wunlatch();
            bufferPoolManager.unpinPage(parentPageId, true);
        }
        return true;
    }

    private boolean mergeLeafNodes(int parentPageId, int sepIdx, int leftPageId, int rightPageId) {
        List<IndexEntry> rightEntries;
        int rightNextLeaf;

        Page rightPage = bufferPoolManager.fetchPage(rightPageId);
        rightPage.rlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightPage);
            rightEntries = right.getAllLeafEntries();
            rightNextLeaf = right.getNextLeafPageId();
        } finally {
            rightPage.runlatch();
            bufferPoolManager.unpinPage(rightPageId, false);
        }

        Page leftPage = bufferPoolManager.fetchPage(leftPageId);
        leftPage.wlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftPage);
            for (IndexEntry entry : rightEntries) {
                left.insertLeafAt(left.getKeyCount(), entry.key(), entry.recordId());
            }
            left.setNextLeafPageId(rightNextLeaf);
        } finally {
            leftPage.wunlatch();
            bufferPoolManager.unpinPage(leftPageId, true);
        }

        if (rightNextLeaf != -1) {
            Page nextLeafPage = bufferPoolManager.fetchPage(rightNextLeaf);
            nextLeafPage.wlatch();
            try {
                BPlusTreePage.wrap(nextLeafPage).setPrevLeafPageId(leftPageId);
            } finally {
                nextLeafPage.wunlatch();
                bufferPoolManager.unpinPage(rightNextLeaf, true);
            }
        }

        bufferPoolManager.deletePage(rightPageId);
        return removeSeparatorFromParent(parentPageId, sepIdx, leftPageId);
    }

    private boolean tryBorrowInternalFromLeft(int parentPageId, int sepIdx, int leftPageId, int childPageId) {
        BPlusTreePage.InternalEntry lastLeft;
        Page leftPage = bufferPoolManager.fetchPage(leftPageId);
        leftPage.rlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftPage);
            int count = left.getKeyCount();
            if (count <= 1) {
                return false;
            }
            lastLeft = left.getInternalEntry(count - 1);
            int entryBytes = BPlusTreePage.SLOT_SIZE + BPlusTreePage.internalEntryPayloadBytes(lastLeft.key());
            if (!canSiblingLend(left, entryBytes)) {
                return false;
            }
        } finally {
            leftPage.runlatch();
            bufferPoolManager.unpinPage(leftPageId, false);
        }

        BPlusTreePage.InternalEntry parentSep;
        Page parentPage = bufferPoolManager.fetchPage(parentPageId);
        parentPage.rlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentPage);
            parentSep = parent.getInternalEntry(sepIdx);
            if (!parent.canReplaceInternalKeyAt(sepIdx, lastLeft.key())) {
                return false;
            }
        } finally {
            parentPage.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        Page childPage = bufferPoolManager.fetchPage(childPageId);
        childPage.rlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childPage);
            if (child.getKeyCount() >= maxKeys || !child.canFitInternalEntry(parentSep.key())) {
                return false;
            }
        } finally {
            childPage.runlatch();
            bufferPoolManager.unpinPage(childPageId, false);
        }

        // Execute internal rotation from left sibling through parent
        Page leftMut = bufferPoolManager.fetchPage(leftPageId);
        leftMut.wlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftMut);
            left.removeAt(left.getKeyCount() - 1);
        } finally {
            leftMut.wunlatch();
            bufferPoolManager.unpinPage(leftPageId, true);
        }

        Page childMut = bufferPoolManager.fetchPage(childPageId);
        childMut.wlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childMut);
            int oldFirstChild = child.getFirstChildPageId();
            child.setFirstChildPageId(lastLeft.rightChildPageId());
            child.insertInternalAt(0, parentSep.key(), parentSep.recordId(), oldFirstChild);
        } finally {
            childMut.wunlatch();
            bufferPoolManager.unpinPage(childPageId, true);
        }

        Page parentMut = bufferPoolManager.fetchPage(parentPageId);
        parentMut.wlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentMut);
            parent.setInternalKeyAt(sepIdx, lastLeft.key(), lastLeft.recordId());
        } finally {
            parentMut.wunlatch();
            bufferPoolManager.unpinPage(parentPageId, true);
        }
        return true;
    }

    private boolean tryBorrowInternalFromRight(int parentPageId, int sepIdx, int childPageId, int rightPageId) {
        BPlusTreePage.InternalEntry firstRight;
        int rightFirstChild;
        Page rightPage = bufferPoolManager.fetchPage(rightPageId);
        rightPage.rlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightPage);
            int count = right.getKeyCount();
            if (count <= 1) {
                return false;
            }
            firstRight = right.getInternalEntry(0);
            rightFirstChild = right.getFirstChildPageId();
            int entryBytes = BPlusTreePage.SLOT_SIZE + BPlusTreePage.internalEntryPayloadBytes(firstRight.key());
            if (!canSiblingLend(right, entryBytes)) {
                return false;
            }
        } finally {
            rightPage.runlatch();
            bufferPoolManager.unpinPage(rightPageId, false);
        }

        BPlusTreePage.InternalEntry parentSep;
        Page parentPage = bufferPoolManager.fetchPage(parentPageId);
        parentPage.rlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentPage);
            parentSep = parent.getInternalEntry(sepIdx);
            if (!parent.canReplaceInternalKeyAt(sepIdx, firstRight.key())) {
                return false;
            }
        } finally {
            parentPage.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        Page childPage = bufferPoolManager.fetchPage(childPageId);
        childPage.rlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childPage);
            if (child.getKeyCount() >= maxKeys || !child.canFitInternalEntry(parentSep.key())) {
                return false;
            }
        } finally {
            childPage.runlatch();
            bufferPoolManager.unpinPage(childPageId, false);
        }

        // Execute internal rotation from right sibling through parent
        Page rightMut = bufferPoolManager.fetchPage(rightPageId);
        rightMut.wlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightMut);
            right.removeAt(0);
            right.setFirstChildPageId(firstRight.rightChildPageId());
        } finally {
            rightMut.wunlatch();
            bufferPoolManager.unpinPage(rightPageId, true);
        }

        Page childMut = bufferPoolManager.fetchPage(childPageId);
        childMut.wlatch();
        try {
            BPlusTreePage child = BPlusTreePage.wrap(childMut);
            child.insertInternalAt(child.getKeyCount(), parentSep.key(), parentSep.recordId(), rightFirstChild);
        } finally {
            childMut.wunlatch();
            bufferPoolManager.unpinPage(childPageId, true);
        }

        Page parentMut = bufferPoolManager.fetchPage(parentPageId);
        parentMut.wlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentMut);
            parent.setInternalKeyAt(sepIdx, firstRight.key(), firstRight.recordId());
        } finally {
            parentMut.wunlatch();
            bufferPoolManager.unpinPage(parentPageId, true);
        }
        return true;
    }

    private boolean mergeInternalNodes(int parentPageId, int sepIdx, int leftPageId, int rightPageId) {
        BPlusTreePage.InternalEntry parentSep;
        Page parentProbe = bufferPoolManager.fetchPage(parentPageId);
        parentProbe.rlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentProbe);
            parentSep = parent.getInternalEntry(sepIdx);
        } finally {
            parentProbe.runlatch();
            bufferPoolManager.unpinPage(parentPageId, false);
        }

        int rightFirstChild;
        List<BPlusTreePage.InternalEntry> rightEntries;
        Page rightPage = bufferPoolManager.fetchPage(rightPageId);
        rightPage.rlatch();
        try {
            BPlusTreePage right = BPlusTreePage.wrap(rightPage);
            rightFirstChild = right.getFirstChildPageId();
            rightEntries = right.getAllInternalEntries();
        } finally {
            rightPage.runlatch();
            bufferPoolManager.unpinPage(rightPageId, false);
        }

        Page leftPage = bufferPoolManager.fetchPage(leftPageId);
        leftPage.wlatch();
        try {
            BPlusTreePage left = BPlusTreePage.wrap(leftPage);
            left.insertInternalAt(left.getKeyCount(), parentSep.key(), parentSep.recordId(), rightFirstChild);
            for (BPlusTreePage.InternalEntry entry : rightEntries) {
                left.insertInternalAt(left.getKeyCount(), entry.key(), entry.recordId(), entry.rightChildPageId());
            }
        } finally {
            leftPage.wunlatch();
            bufferPoolManager.unpinPage(leftPageId, true);
        }

        bufferPoolManager.deletePage(rightPageId);
        return removeSeparatorFromParent(parentPageId, sepIdx, leftPageId);
    }

    private boolean removeSeparatorFromParent(int parentPageId, int sepIdx, int mergedChildPageId) {
        boolean collapseRoot = false;
        boolean parentUnderflow = false;

        Page parentPage = bufferPoolManager.fetchPage(parentPageId);
        parentPage.wlatch();
        try {
            BPlusTreePage parent = BPlusTreePage.wrap(parentPage);
            parent.removeAt(sepIdx);
            if (parentPageId == rootPageId) {
                collapseRoot = (parent.getKeyCount() == 0);
            } else {
                parentUnderflow = isNodeUnderflow(parent);
            }
        } finally {
            parentPage.wunlatch();
            bufferPoolManager.unpinPage(parentPageId, true);
        }

        if (collapseRoot) {
            byte[] childSnapshot = new byte[bufferPoolManager.getDiskManager().getPageSize()];
            Page childPage = bufferPoolManager.fetchPage(mergedChildPageId);
            childPage.rlatch();
            try {
                System.arraycopy(childPage.getData(), 0, childSnapshot, 0, childSnapshot.length);
            } finally {
                childPage.runlatch();
                bufferPoolManager.unpinPage(mergedChildPageId, false);
            }

            Page rootPage = bufferPoolManager.fetchPage(rootPageId);
            rootPage.wlatch();
            try {
                BPlusTreePage rootNode = BPlusTreePage.wrap(rootPage);
                BPlusTreePage snapshotNode = BPlusTreePage.wrap(new Page(mergedChildPageId, childSnapshot));
                rootNode.copyNodeContentsFrom(snapshotNode);
            } finally {
                rootPage.wunlatch();
                bufferPoolManager.unpinPage(rootPageId, true);
            }

            bufferPoolManager.deletePage(mergedChildPageId);
            return false;
        }

        return parentUnderflow;
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
     * Deallocates all pages belonging to this B+Tree and returns them to the buffer pool's page allocator.
     */
    public void freeAllPages() {
        treeLock.writeLock().lock();
        try {
            List<Integer> allPages = new ArrayList<>();
            collectPagesPostOrder(rootPageId, allPages);
            for (int pageId : allPages) {
                bufferPoolManager.deletePage(pageId);
            }
        } finally {
            treeLock.writeLock().unlock();
        }
    }

    private void collectPagesPostOrder(int pageId, List<Integer> collected) {
        List<Integer> children = new ArrayList<>();
        Page page = bufferPoolManager.fetchPage(pageId);
        page.rlatch();
        try {
            BPlusTreePage node = BPlusTreePage.wrap(page);
            if (node.isInternal()) {
                int count = node.getKeyCount();
                for (int i = 0; i <= count; i++) {
                    children.add(node.getChildPageId(i));
                }
            }
        } finally {
            page.runlatch();
            bufferPoolManager.unpinPage(pageId, false);
        }
        for (int childId : children) {
            collectPagesPostOrder(childId, collected);
        }
        collected.add(pageId);
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
