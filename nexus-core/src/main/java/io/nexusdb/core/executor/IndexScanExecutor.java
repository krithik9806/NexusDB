package io.nexusdb.core.executor;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.index.IndexEntry;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.TableHeap;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.transaction.LockId;
import io.nexusdb.core.transaction.LockManager;
import io.nexusdb.core.transaction.LockMode;
import io.nexusdb.core.transaction.Transaction;
import io.nexusdb.core.wal.LogManager;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Physical index-scan operator that probes a {@link BPlusTree} for an equality or range predicate
 * and fetches matching tuples from the owning {@link TableHeap} by {@code RecordId}.
 */
public final class IndexScanExecutor implements Executor {
    private final TableMetadata table;
    private final IndexMetadata index;
    private final BPlusTree indexTree;
    private final Value lowKey;
    private final boolean lowInclusive;
    private final Value highKey;
    private final boolean highInclusive;

    private Iterator<IndexEntry> rangeIterator;
    private LockMode lockMode = LockMode.SHARED;
    private double estimatedCost = 1.35;
    private long estimatedRows = 1L;
    private long actualRows;
    private long elapsedNanos;
    private boolean executed;

    public IndexScanExecutor(
            TableMetadata table,
            IndexMetadata index,
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive) {
        this(
                table,
                index,
                Objects.requireNonNull(table, "table must not be null").getIndexTree(index),
                lowKey,
                lowInclusive,
                highKey,
                highInclusive);
    }

    public IndexScanExecutor(
            TableMetadata table,
            IndexMetadata index,
            BPlusTree indexTree,
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive) {
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.index = Objects.requireNonNull(index, "index must not be null");
        this.indexTree = Objects.requireNonNull(indexTree, "indexTree must not be null");
        this.lowKey = lowKey;
        this.lowInclusive = lowInclusive;
        this.highKey = highKey;
        this.highInclusive = highInclusive;
    }

    public static IndexScanExecutor forEquality(TableMetadata table, IndexMetadata index, Value key) {
        Objects.requireNonNull(key, "key must not be null");
        return new IndexScanExecutor(table, index, key, true, key, true);
    }

    public static IndexScanExecutor forRange(
            TableMetadata table,
            IndexMetadata index,
            Value lowKey,
            boolean lowInclusive,
            Value highKey,
            boolean highInclusive) {
        return new IndexScanExecutor(table, index, lowKey, lowInclusive, highKey, highInclusive);
    }

    public TableMetadata getTable() {
        return table;
    }

    public IndexMetadata getIndex() {
        return index;
    }

    public Value getLowKey() {
        return lowKey;
    }

    public boolean isLowInclusive() {
        return lowInclusive;
    }

    public Value getHighKey() {
        return highKey;
    }

    public boolean isHighInclusive() {
        return highInclusive;
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public void setLockMode(LockMode lockMode) {
        this.lockMode = Objects.requireNonNull(lockMode, "lockMode must not be null");
    }

    public boolean isEqualityLookup() {
        return lowKey != null
                && highKey != null
                && lowInclusive
                && highInclusive
                && !lowKey.isNull()
                && !highKey.isNull()
                && lowKey.compareTo(highKey) == 0;
    }

    @Override
    public void init() {
        long start = System.nanoTime();
        this.actualRows = 0L;
        this.executed = true;
        try {
            if ((lowKey != null && lowKey.isNull()) || (highKey != null && highKey.isNull())) {
                this.rangeIterator = Collections.emptyIterator();
                return;
            }
            this.rangeIterator = indexTree.rangeScan(lowKey, lowInclusive, highKey, highInclusive);
        } finally {
            this.elapsedNanos = System.nanoTime() - start;
        }
    }

    @Override
    public Tuple next() {
        if (rangeIterator == null) {
            throw new IllegalStateException("IndexScanExecutor.init() must be called before next()");
        }
        long start = System.nanoTime();
        try {
            TableHeap heap = table.getTableHeap();
            LogManager lm = heap.getBufferPoolManager().getLogManager();
            Transaction txn = (lm != null && lm.getThreadTransactionContext() instanceof Transaction t) ? t : null;
            LockManager lockMgr = txn != null ? txn.getLockManager() : null;
            int colIdx = table.getSchema().getColumnIndex(index.columnName());

            while (rangeIterator.hasNext()) {
                IndexEntry entry = rangeIterator.next();
                if (entry.key().isNull()) {
                    continue;
                }
                if (lockMgr != null) {
                    lockMgr.acquire(txn, LockId.row(table.getName(), entry.recordId()), lockMode);
                }
                Tuple tuple = heap.get(entry.recordId());
                if (tuple != null && matchesBounds(tuple.getValue(colIdx))) {
                    actualRows++;
                    return tuple;
                }
            }
            return null;
        } finally {
            elapsedNanos += System.nanoTime() - start;
        }
    }

    private boolean matchesBounds(Value val) {
        if (val == null || val.isNull()) {
            return false;
        }
        if (lowKey != null && !lowKey.isNull()) {
            int cmp = val.compareTo(lowKey);
            if (lowInclusive ? cmp < 0 : cmp <= 0) {
                return false;
            }
        }
        if (highKey != null && !highKey.isNull()) {
            int cmp = val.compareTo(highKey);
            if (highInclusive ? cmp > 0 : cmp >= 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        this.rangeIterator = null;
    }

    @Override
    public Schema getOutputSchema() {
        return table.getSchema();
    }

    @Override
    public double getEstimatedCost() {
        return estimatedCost;
    }

    @Override
    public long getEstimatedRows() {
        return estimatedRows;
    }

    @Override
    public void setEstimates(double estimatedCost, long estimatedRows) {
        this.estimatedCost = estimatedCost;
        this.estimatedRows = estimatedRows;
    }

    @Override
    public long getActualRows() {
        return actualRows;
    }

    @Override
    public long getElapsedNanos() {
        return elapsedNanos;
    }

    @Override
    public boolean isExecuted() {
        return executed;
    }

    public String formatPredicate() {
        String col = index.columnName();
        if (isEqualityLookup()) {
            return col + " = " + formatValue(lowKey);
        }
        StringBuilder sb = new StringBuilder();
        if (lowKey != null) {
            sb.append(col).append(lowInclusive ? " >= " : " > ").append(formatValue(lowKey));
        }
        if (highKey != null) {
            if (!sb.isEmpty()) {
                sb.append(" AND ");
            }
            sb.append(col).append(highInclusive ? " <= " : " < ").append(formatValue(highKey));
        }
        return sb.isEmpty() ? "ALL" : sb.toString();
    }

    private static String formatValue(Value v) {
        if (v == null || v.isNull()) {
            return "NULL";
        }
        return switch (v) {
            case Value.IntValue(int val) -> Integer.toString(val);
            case Value.BigIntValue(long val) -> Long.toString(val);
            case Value.DoubleValue(double val) -> Double.toString(val);
            case Value.BooleanValue(boolean val) -> val ? "TRUE" : "FALSE";
            case Value.VarcharValue(String s) -> "'" + s + "'";
            case Value.NullValue ignored -> "NULL";
        };
    }

    @Override
    public String explain(int indent) {
        return "  ".repeat(Math.max(0, indent))
                + "IndexScan(table=" + table.getName()
                + ", index=" + index.indexName()
                + ", predicate=" + formatPredicate() + ")"
                + formatMetrics();
    }
}
