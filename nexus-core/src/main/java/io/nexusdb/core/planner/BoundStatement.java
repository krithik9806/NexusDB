package io.nexusdb.core.planner;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.record.Schema;

import java.util.List;
import java.util.Objects;

/**
 * Sealed hierarchy of semantically validated statements ready for logical/physical planning.
 */
public sealed interface BoundStatement
        permits BoundStatement.BoundCreateDatabase,
                BoundStatement.BoundCreateTable,
                BoundStatement.BoundDropTable,
                BoundStatement.BoundInsert,
                BoundStatement.BoundSelect,
                BoundStatement.BoundUpdate,
                BoundStatement.BoundDelete,
                BoundStatement.BoundExplain {

    record BoundCreateDatabase(String databaseName) implements BoundStatement {
        public BoundCreateDatabase {
            Objects.requireNonNull(databaseName, "databaseName must not be null");
        }
    }

    record BoundCreateTable(String tableName, Schema schema) implements BoundStatement {
        public BoundCreateTable {
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(schema, "schema must not be null");
        }
    }

    record BoundDropTable(String tableName) implements BoundStatement {
        public BoundDropTable {
            Objects.requireNonNull(tableName, "tableName must not be null");
        }
    }

    /**
     * Bound {@code INSERT} where each row's expressions are ordered and aligned 1-to-1 with
     * {@code table.getSchema().getColumns()}.
     */
    record BoundInsert(
            TableMetadata table,
            List<List<BoundExpr>> rows) implements BoundStatement {
        public BoundInsert {
            Objects.requireNonNull(table, "table must not be null");
            Objects.requireNonNull(rows, "rows must not be null");
            rows = rows.stream().map(List::copyOf).toList();
        }
    }

    record BoundProjection(BoundExpr expr, String alias) {
        public BoundProjection {
            Objects.requireNonNull(expr, "expr must not be null");
            Objects.requireNonNull(alias, "alias must not be null");
        }
    }

    record BoundOrderBy(BoundExpr expr, boolean ascending) {
        public BoundOrderBy {
            Objects.requireNonNull(expr, "expr must not be null");
        }
    }

    record BoundSelect(
            TableMetadata table,
            String tableAlias,
            List<BoundProjection> projections,
            Schema outputSchema,
            BoundExpr where,
            List<BoundOrderBy> orderBy,
            Integer limit) implements BoundStatement {
        public BoundSelect {
            Objects.requireNonNull(table, "table must not be null");
            Objects.requireNonNull(projections, "projections must not be null");
            Objects.requireNonNull(outputSchema, "outputSchema must not be null");
            Objects.requireNonNull(orderBy, "orderBy must not be null");
            projections = List.copyOf(projections);
            orderBy = List.copyOf(orderBy);
        }
    }

    record BoundAssignment(int columnIndex, String columnName, BoundExpr value) {
        public BoundAssignment {
            if (columnIndex < 0) {
                throw new IllegalArgumentException("columnIndex must be >= 0");
            }
            Objects.requireNonNull(columnName, "columnName must not be null");
            Objects.requireNonNull(value, "value must not be null");
        }
    }

    record BoundUpdate(
            TableMetadata table,
            List<BoundAssignment> assignments,
            BoundExpr where) implements BoundStatement {
        public BoundUpdate {
            Objects.requireNonNull(table, "table must not be null");
            Objects.requireNonNull(assignments, "assignments must not be null");
            assignments = List.copyOf(assignments);
        }
    }

    record BoundDelete(
            TableMetadata table,
            BoundExpr where) implements BoundStatement {
        public BoundDelete {
            Objects.requireNonNull(table, "table must not be null");
        }
    }

    record BoundExplain(
            boolean analyze,
            BoundStatement statement) implements BoundStatement {
        public BoundExplain {
            Objects.requireNonNull(statement, "statement must not be null");
        }
    }
}
