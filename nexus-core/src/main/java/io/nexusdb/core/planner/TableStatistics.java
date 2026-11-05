package io.nexusdb.core.planner;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.record.TableHeap;

import java.util.Objects;

/**
 * Cardinality and physical storage statistics for a table used by {@link CostBasedPlanner}.
 */
public record TableStatistics(long rowCount, int pageCount) {
    public TableStatistics {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be >= 0, got: " + rowCount);
        }
        if (pageCount < 1) {
            throw new IllegalArgumentException("pageCount must be >= 1, got: " + pageCount);
        }
    }

    public static TableStatistics from(TableMetadata table) {
        Objects.requireNonNull(table, "table must not be null");
        return from(table.getTableHeap());
    }

    public static TableStatistics from(TableHeap heap) {
        Objects.requireNonNull(heap, "heap must not be null");
        return new TableStatistics(heap.getLiveRowCount(), Math.max(1, heap.getPageCount()));
    }
}
