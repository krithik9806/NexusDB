package io.nexusdb.core.index;

import io.nexusdb.core.page.RecordId;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Forward range-scan iterator over a {@link BPlusTree} that walks the doubly-linked leaf chain
 * one page at a time without holding buffer pool pins across caller iterations.
 */
public final class BPlusTreeRangeIterator implements Iterator<IndexEntry>, Iterable<IndexEntry> {
    private final BPlusTree tree;
    private final Value lowKey;
    private final boolean lowInclusive;
    private final Value highKey;
    private final boolean highInclusive;

    private final List<IndexEntry> currentBatch = new ArrayList<>();
    private int batchIndex = 0;
    private int nextLeafPageId;
    private boolean firstLeaf = true;

    BPlusTreeRangeIterator(
            BPlusTree tree,
             int startLeafPageId,
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive) {
        this.tree = tree;
        this.lowKey = lowKey;
        this.lowInclusive = lowInclusive;
        this.highKey = highKey;
        this.highInclusive = highInclusive;
        this.nextLeafPageId = startLeafPageId;
        advanceLeafBatch();
    }

    private void advanceLeafBatch() {
        currentBatch.clear();
        batchIndex = 0;

        while (nextLeafPageId != -1 && currentBatch.isEmpty()) {
            int pageToRead = nextLeafPageId;
            boolean isFirst = firstLeaf;
            firstLeaf = false;
            nextLeafPageId = tree.readRangeLeafBatch(
                    pageToRead, isFirst, lowKey, lowInclusive, highKey, highInclusive, currentBatch);
        }
    }

    @Override
    public boolean hasNext() {
        if (batchIndex < currentBatch.size()) {
            return true;
        }
        if (nextLeafPageId != -1) {
            advanceLeafBatch();
        }
        return batchIndex < currentBatch.size();
    }

    @Override
    public IndexEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more entries in B+Tree range scan");
        }
        return currentBatch.get(batchIndex++);
    }

    @Override
    public Iterator<IndexEntry> iterator() {
        return this;
    }

    /**
     * Collects all remaining {@link IndexEntry} items in this range scan into a list.
     */
    public List<IndexEntry> toList() {
        List<IndexEntry> all = new ArrayList<>();
        while (hasNext()) {
            all.add(next());
        }
        return all;
    }

    /**
     * Collects all remaining {@link RecordId}s in this range scan into a list.
     */
    public List<RecordId> recordIds() {
        List<RecordId> rids = new ArrayList<>();
        while (hasNext()) {
            rids.add(next().recordId());
        }
        return rids;
    }
}
