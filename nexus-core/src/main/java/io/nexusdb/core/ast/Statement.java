package io.nexusdb.core.ast;

import java.util.List;
import java.util.Objects;

/**
 * Sealed AST hierarchy for top-level SQL statements in NexusDB.
 */
public sealed interface Statement
        permits Statement.CreateDatabaseStatement,
                Statement.CreateTableStatement,
                Statement.DropTableStatement,
                Statement.CreateIndexStatement,
                Statement.DropIndexStatement,
                Statement.InsertStatement,
                Statement.SelectStatement,
                Statement.UpdateStatement,
                Statement.DeleteStatement,
                Statement.ExplainStatement {

    /**
     * {@code CREATE DATABASE <databaseName>}
     */
    record CreateDatabaseStatement(String databaseName) implements Statement {
        public CreateDatabaseStatement {
            Objects.requireNonNull(databaseName, "databaseName must not be null");
            if (databaseName.isBlank()) {
                throw new IllegalArgumentException("databaseName must not be blank");
            }
        }
    }

    /**
     * {@code CREATE TABLE <tableName> (<columns>)}
     */
    record CreateTableStatement(String tableName, List<ColumnDef> columns) implements Statement {
        public CreateTableStatement {
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(columns, "columns must not be null");
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("CREATE TABLE must define at least one column");
            }
            columns = List.copyOf(columns);
        }
    }

    /**
     * {@code DROP TABLE <tableName>}
     */
    record DropTableStatement(String tableName) implements Statement {
        public DropTableStatement {
            Objects.requireNonNull(tableName, "tableName must not be null");
            if (tableName.isBlank()) {
                throw new IllegalArgumentException("tableName must not be blank");
            }
        }
    }

    /**
     * {@code CREATE [UNIQUE] INDEX <indexName> ON <tableName> (<columnName>)}
     */
    record CreateIndexStatement(
            String indexName,
            String tableName,
            String columnName,
            boolean unique) implements Statement {
        public CreateIndexStatement {
            Objects.requireNonNull(indexName, "indexName must not be null");
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(columnName, "columnName must not be null");
            if (indexName.isBlank() || tableName.isBlank() || columnName.isBlank()) {
                throw new IllegalArgumentException("Index, table, and column names must not be blank");
            }
        }
    }

    /**
     * {@code DROP INDEX <indexName> [ON <tableName>]}
     */
    record DropIndexStatement(String indexName, String tableName) implements Statement {
        public DropIndexStatement(String indexName) {
            this(indexName, null);
        }

        public DropIndexStatement {
            Objects.requireNonNull(indexName, "indexName must not be null");
            if (indexName.isBlank()) {
                throw new IllegalArgumentException("indexName must not be blank");
            }
            if (tableName != null && tableName.isBlank()) {
                throw new IllegalArgumentException("tableName must not be blank");
            }
        }
    }

    /**
     * {@code INSERT INTO <tableName> [(<columns>)] VALUES (<expr>, ...), ...}
     */
    record InsertStatement(
            String tableName,
            List<String> columns,
            List<List<Expr>> rows) implements Statement {
        public InsertStatement {
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(columns, "columns must not be null");
            Objects.requireNonNull(rows, "rows must not be null");
            if (rows.isEmpty()) {
                throw new IllegalArgumentException("INSERT statement must contain at least one row");
            }
            columns = List.copyOf(columns);
            rows = rows.stream().map(List::copyOf).toList();
        }
    }

    /**
     * {@code SELECT <selectItems> FROM <from> [WHERE <where>] [ORDER BY <orderBy>] [LIMIT <limit>]}
     */
    record SelectStatement(
            List<SelectItem> selectItems,
            TableRef from,
            Expr where,
            List<OrderByItem> orderBy,
            Integer limit) implements Statement {
        public SelectStatement {
            Objects.requireNonNull(selectItems, "selectItems must not be null");
            if (selectItems.isEmpty()) {
                throw new IllegalArgumentException("SELECT list must not be empty");
            }
            Objects.requireNonNull(from, "from must not be null");
            Objects.requireNonNull(orderBy, "orderBy must not be null");
            if (limit != null && limit < 0) {
                throw new IllegalArgumentException("LIMIT must be >= 0, got: " + limit);
            }
            selectItems = List.copyOf(selectItems);
            orderBy = List.copyOf(orderBy);
        }
    }

    /**
     * {@code UPDATE <tableName> SET <assignments> [WHERE <where>]}
     */
    record UpdateStatement(
            String tableName,
            List<Assignment> assignments,
            Expr where) implements Statement {
        public UpdateStatement {
            Objects.requireNonNull(tableName, "tableName must not be null");
            Objects.requireNonNull(assignments, "assignments must not be null");
            if (assignments.isEmpty()) {
                throw new IllegalArgumentException("UPDATE must have at least one SET assignment");
            }
            assignments = List.copyOf(assignments);
        }
    }

    /**
     * {@code DELETE FROM <tableName> [WHERE <where>]}
     */
    record DeleteStatement(
            String tableName,
            Expr where) implements Statement {
        public DeleteStatement {
            Objects.requireNonNull(tableName, "tableName must not be null");
            if (tableName.isBlank()) {
                throw new IllegalArgumentException("tableName must not be blank");
            }
        }
    }

    /**
     * {@code EXPLAIN [ANALYZE] <statement>}
     */
    record ExplainStatement(
            boolean analyze,
            Statement statement) implements Statement {
        public ExplainStatement {
            Objects.requireNonNull(statement, "statement must not be null");
            if (statement instanceof ExplainStatement) {
                throw new IllegalArgumentException("Cannot nest EXPLAIN inside EXPLAIN");
            }
        }
    }
}
