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
                Statement.InsertStatement {

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
}
