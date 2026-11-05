package io.nexusdb.core.engine;

import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.buffer.BufferPoolManager;
import io.nexusdb.core.catalog.Catalog;
import io.nexusdb.core.executor.ExecutionPlanner;
import io.nexusdb.core.executor.Executor;
import io.nexusdb.core.parser.Parser;
import io.nexusdb.core.planner.Binder;
import io.nexusdb.core.planner.BoundStatement;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.storage.DiskManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Embedded relational database facade wiring {@link DiskManager}, {@link BufferPoolManager},
 * {@link Catalog}, {@link Parser}, {@link Binder}, {@link ExecutionPlanner}, and Volcano {@link Executor}s.
 *
 * <p>Thread-Safety Invariant:
 * Statement execution through {@link #execute(String)} and {@link #executeScript(String)} is synchronized
 * on {@code executionLock} until transaction concurrency control (Strict 2PL in Phase 4) is introduced.
 */
public final class Database implements AutoCloseable {
    public static final int DEFAULT_BUFFER_POOL_SIZE = 128;
    public static final String DEFAULT_DB_FILENAME = "nexus.db";

    private static final List<Column> EXPLAIN_COLUMNS = List.of(
            new Column("plan", DataType.VARCHAR, 255, false)
    );

    private final DiskManager diskManager;
    private final BufferPoolManager bufferPoolManager;
    private final Catalog catalog;
    private final Binder binder;
    private final ExecutionPlanner executionPlanner;
    private final Object executionLock = new Object();
    private volatile boolean closed;

    private Database(DiskManager diskManager, BufferPoolManager bufferPoolManager, Catalog catalog) {
        this.diskManager = diskManager;
        this.bufferPoolManager = bufferPoolManager;
        this.catalog = catalog;
        this.binder = new Binder(catalog);
        this.executionPlanner = new ExecutionPlanner();
    }

    /**
     * Opens or creates a NexusDB instance at {@code path} (which may be either a database file path
     * or a directory path in which {@code nexus.db} will be created).
     */
    public static Database open(Path path) {
        return open(path, DEFAULT_BUFFER_POOL_SIZE);
    }

    public static Database open(Path path, int bufferPoolSize) {
        Objects.requireNonNull(path, "path must not be null");
        Path dbFile = resolveDatabaseFile(path);
        DiskManager dm = DiskManager.open(dbFile);
        try {
            BufferPoolManager bpm = new BufferPoolManager(bufferPoolSize, dm);
            Catalog catalog = Catalog.open(bpm);
            return new Database(dm, bpm, catalog);
        } catch (RuntimeException e) {
            dm.close();
            throw e;
        }
    }

    private static Path resolveDatabaseFile(Path path) {
        if (Files.exists(path) && Files.isDirectory(path)) {
            return path.resolve(DEFAULT_DB_FILENAME);
        }
        String fileName = path.getFileName() != null ? path.getFileName().toString() : "";
        if (!Files.exists(path) && !fileName.contains(".")) {
            return path.resolve(DEFAULT_DB_FILENAME);
        }
        return path;
    }

    /**
     * Executes one or more SQL statements separated by {@code ;} and returns the {@link ResultSet}
     * of the final statement.
     */
    public ResultSet execute(String sql) {
        List<ResultSet> results = executeScript(sql);
        return results.get(results.size() - 1);
    }

    /**
     * Executes all semicolon-separated statements in {@code sqlScript} sequentially and returns
     * their corresponding {@link ResultSet}s.
     */
    public List<ResultSet> executeScript(String sqlScript) {
        Objects.requireNonNull(sqlScript, "sqlScript must not be null");
        synchronized (executionLock) {
            ensureOpen();
            List<Statement> statements = Parser.parseScript(sqlScript);
            List<ResultSet> results = new ArrayList<>(statements.size());
            for (Statement stmt : statements) {
                results.add(executeStatementLocked(stmt));
            }
            return List.copyOf(results);
        }
    }

    private ResultSet executeStatementLocked(Statement statement) {
        long startNanos = System.nanoTime();
        BoundStatement bound = binder.bind(statement);
        return executeBoundLocked(bound, startNanos);
    }

    private ResultSet executeBoundLocked(BoundStatement bound, long startNanos) {
        return switch (bound) {
            case BoundStatement.BoundCreateDatabase(String dbName) -> {
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "CREATE DATABASE " + dbName);
            }
            case BoundStatement.BoundCreateTable(String tableName, var schema) -> {
                catalog.createTable(tableName, schema);
                for (Column col : schema.getColumns()) {
                    if (col.primaryKey()) {
                        String pkIndexName = "pk_" + tableName + "_" + col.name();
                        catalog.createIndex(tableName, pkIndexName, col.name(), true);
                    }
                }
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "CREATE TABLE " + tableName);
            }
            case BoundStatement.BoundDropTable(String tableName) -> {
                catalog.dropTable(tableName);
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "DROP TABLE " + tableName);
            }
            case BoundStatement.BoundCreateIndex(
                    String indexName,
                    String tableName,
                    String columnName,
                    boolean unique) -> {
                catalog.createIndex(tableName, indexName, columnName, unique);
                long elapsed = System.nanoTime() - startNanos;
                String tag = (unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ") + indexName;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, tag);
            }
            case BoundStatement.BoundDropIndex(String indexName, String tableName) -> {
                catalog.dropIndex(tableName, indexName);
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "DROP INDEX " + indexName);
            }
            case BoundStatement.BoundSelect select -> {
                try (Executor executor = executionPlanner.plan(select)) {
                    executor.init();
                    List<Tuple> rows = new ArrayList<>();
                    Tuple tuple;
                    while ((tuple = executor.next()) != null) {
                        rows.add(tuple);
                    }
                    long elapsed = System.nanoTime() - startNanos;
                    yield new ResultSet(
                            executor.getOutputSchema().getColumns(),
                            rows,
                            rows.size(),
                            elapsed,
                            "SELECT " + rows.size());
                }
            }
            case BoundStatement.BoundInsert insert -> executeDmlLocked("INSERT", insert, startNanos);
            case BoundStatement.BoundUpdate update -> executeDmlLocked("UPDATE", update, startNanos);
            case BoundStatement.BoundDelete delete -> executeDmlLocked("DELETE", delete, startNanos);
            case BoundStatement.BoundExplain(boolean analyze, BoundStatement inner) ->
                    executeExplainLocked(analyze, inner, startNanos);
        };
    }

    private ResultSet executeDmlLocked(String tag, BoundStatement dml, long startNanos) {
        try (Executor executor = executionPlanner.plan(dml)) {
            executor.init();
            Tuple resultTuple = executor.next();
            int affected = resultTuple != null ? ((Value.IntValue) resultTuple.getValue(0)).value() : 0;
            long elapsed = System.nanoTime() - startNanos;
            return new ResultSet(List.of(), List.of(), affected, elapsed, tag + " " + affected);
        }
    }

    private ResultSet executeExplainLocked(boolean analyze, BoundStatement inner, long startNanos) {
        String planText = switch (inner) {
            case BoundStatement.BoundSelect ignored -> explainBoundDmlOrSelect(analyze, inner, startNanos);
            case BoundStatement.BoundInsert ignored -> explainBoundDmlOrSelect(analyze, inner, startNanos);
            case BoundStatement.BoundUpdate ignored -> explainBoundDmlOrSelect(analyze, inner, startNanos);
            case BoundStatement.BoundDelete ignored -> explainBoundDmlOrSelect(analyze, inner, startNanos);
            default -> inner.getClass().getSimpleName();
        };

        List<Tuple> planRows = new ArrayList<>();
        for (String line : planText.split("\n")) {
            planRows.add(new Tuple(Value.ofVarchar(line)));
        }
        long elapsed = System.nanoTime() - startNanos;
        return new ResultSet(EXPLAIN_COLUMNS, planRows, 0, elapsed, "EXPLAIN");
    }

    private String explainBoundDmlOrSelect(boolean analyze, BoundStatement inner, long startNanos) {
        try (Executor exec = executionPlanner.plan(inner)) {
            if (!analyze) {
                return exec.explain(0);
            }
            exec.init();
            while (exec.next() != null) {
                // Consume all tuples to record actualRows and timeMs at each operator node
            }
            return exec.explain(0);
        }
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public BufferPoolManager getBufferPoolManager() {
        return bufferPoolManager;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Database is closed");
        }
    }

    @Override
    public void close() {
        synchronized (executionLock) {
            if (!closed) {
                closed = true;
                try {
                    bufferPoolManager.close();
                } finally {
                    diskManager.close();
                }
            }
        }
    }
}
