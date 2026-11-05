package io.nexusdb.core.planner;

import io.nexusdb.core.ast.JoinType;
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
                BoundStatement.BoundCreateIndex,
                BoundStatement.BoundDropIndex,
                BoundStatement.BoundInsert,
                BoundStatement.BoundSelect,
                BoundStatement.BoundUpdate,
                BoundStatement.BoundDelete,
                BoundStatement.BoundBegin,
                BoundStatement.BoundCommit,
                BoundStatement.BoundRollback,
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

    record BoundCreateIndex(
            String indexName,
            String tableName,
            String columnName,
            boolean unique) implements BoundStatement {
        public BoundCreateIndex {
            Objects.requireNonNull(indexName, "indexName must not be null");
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(columnName, "columnName must not be null");
        }
    }

    record BoundDropIndex(String indexName, String tableName) implements BoundStatement {
        public BoundDropIndex {
            Objects.requireNonNull(indexName, "indexName must not be null");
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

    record BoundJoin(
            JoinType joinType,
            TableMetadata rightTable,
            String rightAlias,
            BoundExpr condition,
            int rightStartColumnIndex) {
        public BoundJoin {
            Objects.requireNonNull(joinType, "joinType must not be null");
            Objects.requireNonNull(rightTable, "rightTable must not be null");
            Objects.requireNonNull(condition, "condition must not be null");
            if (rightStartColumnIndex < 0) {
                throw new IllegalArgumentException("rightStartColumnIndex must be >= 0");
            }
        }
    }

    record BoundSelect(
            TableMetadata table,
            String tableAlias,
            List<BoundJoin> joins,
            List<BoundProjection> projections,
            Schema outputSchema,
            BoundExpr where,
            List<BoundExpr> groupBy,
            List<BoundExpr.BoundAggregate> aggregates,
            BoundExpr having,
            List<BoundOrderBy> orderBy,
            Integer limit) implements BoundStatement {
        public BoundSelect(
                TableMetadata table,
                String tableAlias,
                List<BoundProjection> projections,
                Schema outputSchema,
                BoundExpr where,
                List<BoundOrderBy> orderBy,
                Integer limit) {
            this(table, tableAlias, List.of(), projections, outputSchema, where, List.of(), List.of(), null, orderBy, limit);
        }

        public BoundSelect(
                TableMetadata table,
                String tableAlias,
                List<BoundProjection> projections,
                Schema outputSchema,
                BoundExpr where,
                List<BoundExpr> groupBy,
                List<BoundExpr.BoundAggregate> aggregates,
                BoundExpr having,
                List<BoundOrderBy> orderBy,
                Integer limit) {
            this(table, tableAlias, List.of(), projections, outputSchema, where, groupBy, aggregates, having, orderBy, limit);
        }

        public BoundSelect {
            Objects.requireNonNull(table, "table must not be null");
            Objects.requireNonNull(joins, "joins must not be null");
            Objects.requireNonNull(projections, "projections must not be null");
            Objects.requireNonNull(outputSchema, "outputSchema must not be null");
            Objects.requireNonNull(groupBy, "groupBy must not be null");
            Objects.requireNonNull(aggregates, "aggregates must not be null");
            Objects.requireNonNull(orderBy, "orderBy must not be null");
            joins = List.copyOf(joins);
            projections = List.copyOf(projections);
            groupBy = List.copyOf(groupBy);
            aggregates = List.copyOf(aggregates);
            orderBy = List.copyOf(orderBy);
        }

        public boolean isAggregated() {
            return !groupBy.isEmpty() || !aggregates.isEmpty() || having != null;
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

    record BoundBegin() implements BoundStatement {
    }

    record BoundCommit() implements BoundStatement {
    }

    record BoundRollback() implements BoundStatement {
    }

    record BoundExplain(
            boolean analyze,
            BoundStatement statement) implements BoundStatement {
        public BoundExplain {
            Objects.requireNonNull(statement, "statement must not be null");
        }
    }
}
