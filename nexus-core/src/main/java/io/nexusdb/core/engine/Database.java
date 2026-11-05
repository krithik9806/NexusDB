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
import io.nexusdb.core.recovery.RecoveryManager;
import io.nexusdb.core.storage.DiskManager;
import io.nexusdb.core.transaction.Transaction;
import io.nexusdb.core.transaction.TransactionManager;
import io.nexusdb.core.transaction.TransactionState;
import io.nexusdb.core.wal.CheckpointManager;
import io.nexusdb.core.wal.LogManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Embedded relational database facade wiring {@link DiskManager}, {@link LogManager},
 * {@link BufferPoolManager}, {@link CheckpointManager}, {@link Catalog}, {@link TransactionManager},
 * {@link Parser}, {@link Binder}, {@link ExecutionPlanner}, and Volcano {@link Executor}s.
 *
 * <p>Transaction & Concurrency Invariants:
 * <ul>
 *   <li>Statements executed outside an explicit {@code BEGIN ... COMMIT/ROLLBACK} block run in
 *       autocommit mode, wrapping each DML modification in its own durable transaction.</li>
 *   <li>Explicit transactions started via {@code BEGIN} are tracked per calling thread (or passed
 *       explicitly via {@link #execute(Transaction, String)}). If a multi-row DML statement fails
 *       midway (e.g. {@code DuplicateKeyException}), statement-level savepoint undo rolls back only
 *       that statement's changes while keeping the enclosing explicit transaction active.</li>
 * </ul>
 */
public final class Database implements AutoCloseable {
    public static final int DEFAULT_BUFFER_POOL_SIZE = 128;
    public static final String DEFAULT_DB_FILENAME = "nexus.db";

    private static final List<Column> EXPLAIN_COLUMNS = List.of(
            new Column("plan", DataType.VARCHAR, 255, false)
    );

    private final DiskManager diskManager;
    private final LogManager logManager;
    private final BufferPoolManager bufferPoolManager;
    private final CheckpointManager checkpointManager;
    private final Catalog catalog;
    private final TransactionManager transactionManager;
    private final RecoveryManager recoveryManager;
    private final RecoveryManager.RecoveryReport lastRecoveryReport;
    private final Binder binder;
    private final ExecutionPlanner executionPlanner;
    private final Object executionLock = new Object();
    private final ThreadLocal<Transaction> explicitThreadTxn = new ThreadLocal<>();
    private volatile boolean closed;

    private Database(
            DiskManager diskManager,
            LogManager logManager,
            BufferPoolManager bufferPoolManager,
            CheckpointManager checkpointManager,
            Catalog catalog,
            TransactionManager transactionManager,
            RecoveryManager recoveryManager,
            RecoveryManager.RecoveryReport lastRecoveryReport) {
        this.diskManager = diskManager;
        this.logManager = logManager;
        this.bufferPoolManager = bufferPoolManager;
        this.checkpointManager = checkpointManager;
        this.catalog = catalog;
        this.transactionManager = transactionManager;
        this.recoveryManager = recoveryManager;
        this.lastRecoveryReport = lastRecoveryReport;
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
        Path walFile = resolveWalFile(dbFile);
        DiskManager dm = DiskManager.open(dbFile);
        LogManager lm = null;
        try {
            lm = LogManager.open(walFile);
            BufferPoolManager bpm = new BufferPoolManager(bufferPoolSize, dm);
            bpm.setLogManager(lm);
            TransactionManager tm = new TransactionManager(lm);

            // Run ARIES-lite crash recovery (Analysis -> Redo -> Undo) before opening Catalog
            RecoveryManager preCatalogRecovery = new RecoveryManager(bpm, lm, null, tm);
            RecoveryManager.RecoveryReport report = preCatalogRecovery.recover();

            Catalog catalog = Catalog.open(bpm);
            tm.setCatalog(catalog);

            if (report.redoneRecordCount() > 0
                    || report.undoneRecordCount() > 0
                    || !report.loserTransactions().isEmpty()) {
                for (var table : catalog.listTables()) {
                    table.rebuildIndexesFromHeap();
                }
                lm.flush();
                bpm.flushAll();
            }

            CheckpointManager ckpt = new CheckpointManager(lm, bpm, dm, tm::getActiveTransactionTable);
            RecoveryManager rm = new RecoveryManager(bpm, lm, catalog, tm);
            return new Database(dm, lm, bpm, ckpt, catalog, tm, rm, report);
        } catch (RuntimeException e) {
            if (lm != null) {
                try {
                    lm.close();
                } catch (RuntimeException ignored) {
                }
            }
            dm.close();
            throw e;
        }
    }

    public static Path resolveWalFile(Path dbFile) {
        return dbFile.resolveSibling(dbFile.getFileName().toString() + ".wal");
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
     * Executes one or more SQL statements within the context of the specified explicit {@link Transaction}.
     */
    public ResultSet execute(Transaction txn, String sql) {
        Objects.requireNonNull(txn, "txn must not be null");
        Objects.requireNonNull(sql, "sql must not be null");
        if (txn.getState() != TransactionState.ACTIVE) {
            throw new IllegalStateException("Transaction is not active: " + txn);
        }
        Transaction prev = explicitThreadTxn.get();
        explicitThreadTxn.set(txn);
        try {
            return execute(sql);
        } finally {
            if (prev != null && prev.getState() == TransactionState.ACTIVE) {
                explicitThreadTxn.set(prev);
            } else {
                explicitThreadTxn.remove();
            }
        }
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
            case BoundStatement.BoundBegin ignored -> {
                Transaction current = getActiveExplicitThreadTxn();
                if (current != null) {
                    throw new IllegalStateException("Transaction is already active (txnId=" + current.getTxnId() + ")");
                }
                Transaction txn = transactionManager.begin(false);
                explicitThreadTxn.set(txn);
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "BEGIN");
            }
            case BoundStatement.BoundCommit ignored -> {
                Transaction current = getActiveExplicitThreadTxn();
                if (current == null) {
                    throw new IllegalStateException("No active transaction to COMMIT");
                }
                try {
                    transactionManager.commit(current);
                } finally {
                    explicitThreadTxn.remove();
                }
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "COMMIT");
            }
            case BoundStatement.BoundRollback ignored -> {
                Transaction current = getActiveExplicitThreadTxn();
                if (current == null) {
                    throw new IllegalStateException("No active transaction to ROLLBACK");
                }
                try {
                    transactionManager.rollback(current);
                } finally {
                    explicitThreadTxn.remove();
                }
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "ROLLBACK");
            }
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
                logManager.flush();
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "CREATE TABLE " + tableName);
            }
            case BoundStatement.BoundDropTable(String tableName) -> {
                catalog.dropTable(tableName);
                logManager.flush();
                long elapsed = System.nanoTime() - startNanos;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, "DROP TABLE " + tableName);
            }
            case BoundStatement.BoundCreateIndex(
                    String indexName,
                    String tableName,
                    String columnName,
                    boolean unique) -> {
                catalog.createIndex(tableName, indexName, columnName, unique);
                logManager.flush();
                long elapsed = System.nanoTime() - startNanos;
                String tag = (unique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ") + indexName;
                yield new ResultSet(List.of(), List.of(), 0, elapsed, tag);
            }
            case BoundStatement.BoundDropIndex(String indexName, String tableName) -> {
                catalog.dropIndex(tableName, indexName);
                logManager.flush();
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

    private Transaction getActiveExplicitThreadTxn() {
        Transaction txn = explicitThreadTxn.get();
        if (txn != null && txn.getState() != TransactionState.ACTIVE) {
            explicitThreadTxn.remove();
            return null;
        }
        return txn;
    }

    private ResultSet executeDmlLocked(String tag, BoundStatement dml, long startNanos) {
        Transaction explicitTxn = getActiveExplicitThreadTxn();
        if (explicitTxn != null) {
            int savepoint = explicitTxn.getLoggedModificationCount();
            transactionManager.bindToCurrentThread(explicitTxn);
            try (Executor executor = executionPlanner.plan(dml)) {
                executor.init();
                Tuple resultTuple = executor.next();
                int affected = resultTuple != null ? ((Value.IntValue) resultTuple.getValue(0)).value() : 0;
                long elapsed = System.nanoTime() - startNanos;
                return new ResultSet(List.of(), List.of(), affected, elapsed, tag + " " + affected);
            } catch (RuntimeException e) {
                transactionManager.rollbackToSavepoint(explicitTxn, savepoint);
                throw e;
            } finally {
                transactionManager.unbindFromCurrentThread();
            }
        }

        Transaction autoTxn = transactionManager.begin(true);
        try (Executor executor = executionPlanner.plan(dml)) {
            executor.init();
            Tuple resultTuple = executor.next();
            int affected = resultTuple != null ? ((Value.IntValue) resultTuple.getValue(0)).value() : 0;
            transactionManager.commit(autoTxn);
            long elapsed = System.nanoTime() - startNanos;
            return new ResultSet(List.of(), List.of(), affected, elapsed, tag + " " + affected);
        } catch (RuntimeException e) {
            transactionManager.rollback(autoTxn);
            throw e;
        } finally {
            transactionManager.unbindFromCurrentThread();
        }
    }

    private ResultSet executeExplainLocked(boolean analyze, BoundStatement inner, long startNanos) {
        String planText = switch (inner) {
            case BoundStatement.BoundSelect ignored -> explainBoundDmlOrSelect(analyze, inner, false);
            case BoundStatement.BoundInsert ignored -> explainBoundDmlOrSelect(analyze, inner, true);
            case BoundStatement.BoundUpdate ignored -> explainBoundDmlOrSelect(analyze, inner, true);
            case BoundStatement.BoundDelete ignored -> explainBoundDmlOrSelect(analyze, inner, true);
            default -> inner.getClass().getSimpleName();
        };

        List<Tuple> planRows = new ArrayList<>();
        for (String line : planText.split("\n")) {
            planRows.add(new Tuple(Value.ofVarchar(line)));
        }
        long elapsed = System.nanoTime() - startNanos;
        return new ResultSet(EXPLAIN_COLUMNS, planRows, 0, elapsed, "EXPLAIN");
    }

    private String explainBoundDmlOrSelect(boolean analyze, BoundStatement inner, boolean isDml) {
        if (!analyze) {
            try (Executor exec = executionPlanner.plan(inner)) {
                return exec.explain(0);
            }
        }
        if (!isDml) {
            try (Executor exec = executionPlanner.plan(inner)) {
                exec.init();
                while (exec.next() != null) {
                    // Consume all tuples to record actualRows and timeMs at each operator node
                }
                return exec.explain(0);
            }
        }

        Transaction explicitTxn = getActiveExplicitThreadTxn();
        if (explicitTxn != null) {
            int savepoint = explicitTxn.getLoggedModificationCount();
            transactionManager.bindToCurrentThread(explicitTxn);
            try (Executor exec = executionPlanner.plan(inner)) {
                exec.init();
                while (exec.next() != null) {
                }
                return exec.explain(0);
            } catch (RuntimeException e) {
                transactionManager.rollbackToSavepoint(explicitTxn, savepoint);
                throw e;
            } finally {
                transactionManager.unbindFromCurrentThread();
            }
        }

        Transaction autoTxn = transactionManager.begin(true);
        try (Executor exec = executionPlanner.plan(inner)) {
            exec.init();
            while (exec.next() != null) {
            }
            transactionManager.commit(autoTxn);
            return exec.explain(0);
        } catch (RuntimeException e) {
            transactionManager.rollback(autoTxn);
            throw e;
        } finally {
            transactionManager.unbindFromCurrentThread();
        }
    }

    public Transaction beginTransaction() {
        synchronized (executionLock) {
            ensureOpen();
            Transaction txn = transactionManager.begin(false);
            transactionManager.unbindFromCurrentThread();
            return txn;
        }
    }

    public void commitTransaction(Transaction txn) {
        synchronized (executionLock) {
            ensureOpen();
            transactionManager.commit(txn);
        }
    }

    public void rollbackTransaction(Transaction txn) {
        synchronized (executionLock) {
            ensureOpen();
            transactionManager.rollback(txn);
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

    public LogManager getLogManager() {
        return logManager;
    }

    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public RecoveryManager getRecoveryManager() {
        return recoveryManager;
    }

    public RecoveryManager.RecoveryReport getLastRecoveryReport() {
        return lastRecoveryReport;
    }

    public CheckpointManager.CheckpointResult checkpoint() {
        synchronized (executionLock) {
            ensureOpen();
            return checkpointManager.checkpoint();
        }
    }

    public CheckpointManager.CheckpointResult checkpoint(Map<Long, Long> activeTransactions) {
        synchronized (executionLock) {
            ensureOpen();
            return checkpointManager.checkpoint(activeTransactions);
        }
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
                explicitThreadTxn.remove();
                try {
                    bufferPoolManager.close();
                } finally {
                    try {
                        logManager.close();
                    } finally {
                        diskManager.close();
                    }
                }
            }
        }
    }
}
