package io.nexusdb.core.recovery;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.transaction.Transaction;
import io.nexusdb.core.wal.LogReader;
import io.nexusdb.core.wal.LogRecord;
import io.nexusdb.core.wal.LogRecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrashRecoveryHarnessTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void resetFailpoints() {
        CrashFailpoints.clear();
    }

    @Test
    void failpointBeforeCommitWalFlushRollsBackUncommittedCandidateTransaction() throws Exception {
        Path dir = tempDir.resolve("fp_before_commit");
        Files.createDirectories(dir);
        Path dbFile = dir.resolve("crash.db");
        Path truthLog = dir.resolve("truth.log");

        Process proc = spawnWorker(dbFile, truthLog, 1001L, CrashFailpoints.Point.BEFORE_COMMIT_WAL_FLUSH.name(), 2);
        assertTrue(proc.waitFor(15, TimeUnit.SECONDS), "Worker should halt on BEFORE_COMMIT_WAL_FLUSH");
        assertEquals(99, proc.exitValue());

        verifyRecoveredDatabaseMatchesTruth(dbFile, truthLog);
    }

    @Test
    void failpointAfterCommitWalFlushRedoesWinnerTransactionWithoutPageFlush() throws Exception {
        Path dir = tempDir.resolve("fp_after_commit");
        Files.createDirectories(dir);
        Path dbFile = dir.resolve("crash.db");
        Path truthLog = dir.resolve("truth.log");

        Process proc = spawnWorker(dbFile, truthLog, 2002L, CrashFailpoints.Point.AFTER_COMMIT_WAL_FLUSH.name(), 2);
        assertTrue(proc.waitFor(15, TimeUnit.SECONDS), "Worker should halt on AFTER_COMMIT_WAL_FLUSH");
        assertEquals(99, proc.exitValue());

        verifyRecoveredDatabaseMatchesTruth(dbFile, truthLog);
    }

    @Test
    void failpointMidPageWriteRepairsTornPageViaWalRedo() throws Exception {
        Path dir = tempDir.resolve("fp_mid_page_write");
        Files.createDirectories(dir);
        Path dbFile = dir.resolve("crash.db");
        Path truthLog = dir.resolve("truth.log");

        Process proc = spawnWorker(dbFile, truthLog, 3003L, CrashFailpoints.Point.MID_PAGE_WRITE.name(), 1);
        assertTrue(proc.waitFor(15, TimeUnit.SECONDS), "Worker should halt on MID_PAGE_WRITE");
        assertEquals(99, proc.exitValue());

        verifyRecoveredDatabaseMatchesTruth(dbFile, truthLog);
    }

    @Test
    void recoveryIsIdempotentWhenCrashOccursMidUndoPass() throws Exception {
        Path dir = tempDir.resolve("fp_mid_undo");
        Files.createDirectories(dir);
        Path dbFile = dir.resolve("mid_undo.db");
        Path truthLog = dir.resolve("truth.log");

        // 1. Create baseline committed rows + an uncommitted loser transaction with 4 operations
        //    whose dirty pages are flushed to disk (STEAL policy), then close channels without aborting.
        try (Database db = Database.open(dbFile, 16)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        code VARCHAR(32) NOT NULL,
                        balance INT NOT NULL,
                        note VARCHAR(160) NOT NULL
                    );
                    CREATE UNIQUE INDEX idx_accounts_code ON accounts (code);
                    CREATE INDEX idx_accounts_balance ON accounts (balance);
                    """);
            db.execute("INSERT INTO accounts VALUES (1, 'ACC_1', 1000, 'baseline_1');");
            db.execute("INSERT INTO accounts VALUES (2, 'ACC_2', 2000, 'baseline_2');");
            db.checkpoint();

            Transaction loser = db.beginTransaction();
            db.execute(loser, "INSERT INTO accounts VALUES (101, 'LOSER_101', 999, 'loser_insert_1');");
            db.execute(loser, "INSERT INTO accounts VALUES (102, 'LOSER_102', 888, 'loser_insert_2');");
            db.execute(loser, "UPDATE accounts SET balance = -500, note = 'CORRUPTED_BY_LOSER' WHERE id = 1;");
            db.execute(loser, "DELETE FROM accounts WHERE id = 2;");

            // Force STEAL: flush dirty pages containing uncommitted loser changes to disk
            db.getBufferPoolManager().flushAll();
            db.getLogManager().flush();
        }

        // 2. Spawn child JVM that opens the DB and halts mid-Undo after undoing 2 of the 4 loser records
        Process crashMidRecovery = spawnWorker(dbFile, truthLog, 0L, "RECOVER_WITH_UNDO_FAILPOINT", 2);
        assertTrue(crashMidRecovery.waitFor(15, TimeUnit.SECONDS), "Child recovery should halt mid-Undo");
        assertEquals(99, crashMidRecovery.exitValue());

        // Verify WAL now has CLR records from the interrupted first recovery attempt, but no ABORT yet
        List<LogRecord> walAfterInterruptedRecovery = LogReader.readAll(Database.resolveWalFile(dbFile), true);
        assertTrue(walAfterInterruptedRecovery.stream().anyMatch(r -> r.type() == LogRecordType.CLR));

        // 3. Reopen Database in parent JVM; second recovery must follow CLR.undoNextLsn and finish Undo cleanly
        try (Database recovered = Database.open(dbFile, 16)) {
            ResultSet rows = recovered.execute("SELECT id, code, balance, note FROM accounts ORDER BY id ASC;");
            assertEquals(2, rows.rows().size());
            assertEquals(
                    new Tuple(Value.ofInt(1), Value.ofVarchar("ACC_1"), Value.ofInt(1000), Value.ofVarchar("baseline_1")),
                    rows.rows().get(0));
            assertEquals(
                    new Tuple(Value.ofInt(2), Value.ofVarchar("ACC_2"), Value.ofInt(2000), Value.ofVarchar("baseline_2")),
                    rows.rows().get(1));

            TableMetadata table = recovered.getCatalog().getTable("accounts");
            assertEquals(2L, table.getTableHeap().getLiveRowCount());
            for (IndexMetadata idx : table.getIndexes()) {
                table.getIndexTree(idx).verifyInvariants();
            }
        }
    }

    @Test
    void randomProcessKillCrashRecoverySuite() throws Exception {
        boolean slowMode = Boolean.getBoolean("slowTests");
        int iterations = slowMode ? 200 : 12;
        int parallelism = Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors()));

        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        try {
            List<Callable<Void>> tasks = new ArrayList<>(iterations);
            for (int i = 0; i < iterations; i++) {
                final int iter = i;
                tasks.add(() -> {
                    runSingleRandomKillIteration(iter);
                    return null;
                });
            }
            List<Future<Void>> futures = pool.invokeAll(tasks);
            for (Future<Void> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private void runSingleRandomKillIteration(int iter) throws Exception {
        Path iterDir = tempDir.resolve("kill_iter_" + iter);
        Files.createDirectories(iterDir);
        Path dbFile = iterDir.resolve("workload.db");
        Path truthLog = iterDir.resolve("truth.log");
        long seed = 50_000L + iter * 31L;

        // Alternate between random external Process.destroyForcibly() kills and deterministic failpoints
        String mode = switch (iter % 6) {
            case 1 -> CrashFailpoints.Point.BEFORE_COMMIT_WAL_FLUSH.name();
            case 2 -> CrashFailpoints.Point.AFTER_COMMIT_WAL_FLUSH.name();
            case 3 -> CrashFailpoints.Point.MID_PAGE_WRITE.name();
            default -> CrashFailpoints.Point.NONE.name();
        };
        int failpointAfter = 1 + (iter % 3);

        Process proc = spawnWorkerAndWaitForReady(dbFile, truthLog, seed, mode, failpointAfter);
        try {
            if (CrashFailpoints.Point.NONE.name().equals(mode)) {
                Random killRng = new Random(seed ^ 0x5DEECE66DL);
                Thread.sleep(10 + killRng.nextInt(25));
                proc.destroyForcibly();
            }
            if (!proc.waitFor(15, TimeUnit.SECONDS)) {
                proc.destroyForcibly();
                proc.waitFor(5, TimeUnit.SECONDS);
            }
        } finally {
            if (proc.isAlive()) {
                proc.destroyForcibly();
            }
        }

        verifyRecoveredDatabaseMatchesTruth(dbFile, truthLog);
    }

    private Process spawnWorker(
            Path dbFile,
            Path truthLog,
            long seed,
            String failpoint,
            int failpointAfter) throws Exception {
        String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");
        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-Xmx128m",
                "-cp",
                classpath,
                CrashWorkerMain.class.getName(),
                dbFile.toAbsolutePath().toString(),
                truthLog.toAbsolutePath().toString(),
                Long.toString(seed),
                failpoint,
                Integer.toString(failpointAfter));
        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream(true);
        return pb.start();
    }

    private Process spawnWorkerAndWaitForReady(
            Path dbFile,
            Path truthLog,
            long seed,
            String failpoint,
            int failpointAfter) throws Exception {
        Process proc = spawnWorker(dbFile, truthLog, seed, failpoint, failpointAfter);
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8));
        String line = reader.readLine();
        // Drain remaining output asynchronously so child stdout pipe never blocks
        Thread drainThread = new Thread(() -> {
            try {
                while (reader.readLine() != null) {
                    // discard
                }
            } catch (Exception ignored) {
            }
        });
        drainThread.setDaemon(true);
        drainThread.start();
        if (line != null && !line.contains("READY") && !proc.isAlive()) {
            throw new IllegalStateException("Worker exited prematurely with output: " + line);
        }
        return proc;
    }

    private void verifyRecoveredDatabaseMatchesTruth(Path dbFile, Path truthLog) throws Exception {
        Path walFile = Database.resolveWalFile(dbFile);
        Set<Long> walCommittedTxnIds = new HashSet<>();
        if (Files.exists(walFile)) {
            for (LogRecord rec : LogReader.readAll(walFile, true)) {
                if (rec.type() == LogRecordType.COMMIT) {
                    walCommittedTxnIds.add(rec.txnId());
                }
            }
        }

        Map<Integer, CrashWorkerMain.AccountRow> expectedState = reconstructExpectedState(truthLog, walCommittedTxnIds);

        try (Database db = Database.open(dbFile, 16)) {
            if (!db.getCatalog().hasTable("accounts")) {
                assertTrue(expectedState.isEmpty());
                return;
            }

            ResultSet rs = db.execute("SELECT id, code, balance, note FROM accounts ORDER BY id ASC;");
            List<Tuple> actualRows = rs.rows();

            // 1. Verify zero uncommitted loser rows or corrupted loser notes survived recovery
            for (Tuple row : actualRows) {
                int id = ((Value.IntValue) row.getValue(0)).value();
                String code = ((Value.VarcharValue) row.getValue(1)).value();
                int balance = ((Value.IntValue) row.getValue(2)).value();
                String note = ((Value.VarcharValue) row.getValue(3)).value();
                assertTrue(id < 100_000, "Uncommitted loser row ID survived recovery: " + id);
                assertFalse(code.startsWith("UNCOMMITTED_"), "Uncommitted code survived recovery: " + code);
                assertFalse("CORRUPTED_BY_LOSER".equals(note), "Uncommitted update survived recovery on id=" + id);
                assertTrue(balance >= 0, "Negative loser balance survived recovery on id=" + id);
            }

            // 2. Verify exact row count and row-by-row equality with ground-truth committed state
            List<CrashWorkerMain.AccountRow> expectedSorted = expectedState.values().stream()
                    .sorted((a, b) -> Integer.compare(a.id(), b.id()))
                    .toList();
            assertEquals(
                    expectedSorted.size(),
                    actualRows.size(),
                    "Recovered row count mismatch against ground truth");

            for (int i = 0; i < expectedSorted.size(); i++) {
                CrashWorkerMain.AccountRow exp = expectedSorted.get(i);
                Tuple act = actualRows.get(i);
                assertEquals(Value.ofInt(exp.id()), act.getValue(0));
                assertEquals(Value.ofVarchar(exp.code()), act.getValue(1));
                assertEquals(Value.ofInt(exp.balance()), act.getValue(2));
                assertEquals(Value.ofVarchar(exp.note()), act.getValue(3));

                // Verify secondary unique index point lookup matches the heap row
                ResultSet byCode = db.execute("SELECT id, balance FROM accounts WHERE code = '" + exp.code() + "';");
                assertEquals(1, byCode.rows().size(), "Unique index lookup failed for code=" + exp.code());
                assertEquals(Value.ofInt(exp.id()), byCode.rows().get(0).getValue(0));
                assertEquals(Value.ofInt(exp.balance()), byCode.rows().get(0).getValue(1));
            }

            // 3. Verify all B+Tree indexes pass structural invariants and match heap liveRowCount
            TableMetadata table = db.getCatalog().getTable("accounts");
            assertEquals((long) expectedSorted.size(), table.getTableHeap().getLiveRowCount());
            for (IndexMetadata idx : table.getIndexes()) {
                BPlusTree tree = table.getIndexTree(idx);
                tree.verifyInvariants();
            }
        }
    }

    private static Map<Integer, CrashWorkerMain.AccountRow> reconstructExpectedState(
            Path truthLog,
            Set<Long> walCommittedTxnIds) throws Exception {
        Map<Integer, CrashWorkerMain.AccountRow> state = new LinkedHashMap<>();
        if (!Files.exists(truthLog)) {
            return state;
        }

        List<String> lines = Files.readAllLines(truthLog, StandardCharsets.UTF_8);
        Map<Long, String> candidates = new LinkedHashMap<>();
        Set<Long> confirmed = new HashSet<>();

        for (String rawLine : lines) {
            String line = rawLine.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (line.startsWith("CANDIDATE|")) {
                String[] parts = line.split("\\|", 3);
                if (parts.length == 3) {
                    long txnId = Long.parseLong(parts[1]);
                    candidates.put(txnId, parts[2]);
                }
            } else if (line.startsWith("CONFIRMED|")) {
                String[] parts = line.split("\\|", 2);
                if (parts.length == 2) {
                    confirmed.add(Long.parseLong(parts[1]));
                }
            }
        }

        for (Map.Entry<Long, String> entry : candidates.entrySet()) {
            long txnId = entry.getKey();
            // A transaction is durable if CONFIRMED was logged OR if its COMMIT record reached the WAL
            if (confirmed.contains(txnId) || walCommittedTxnIds.contains(txnId)) {
                applyEncodedOps(state, entry.getValue());
            }
        }
        return state;
    }

    private static void applyEncodedOps(
            Map<Integer, CrashWorkerMain.AccountRow> state,
            String opsPayload) {
        if (opsPayload == null || opsPayload.isEmpty()) {
            return;
        }
        for (String op : opsPayload.split(";")) {
            if (op.startsWith("UPSERT:")) {
                String[] fields = op.split(":", 5);
                int id = Integer.parseInt(fields[1]);
                String code = fields[2];
                int balance = Integer.parseInt(fields[3]);
                String note = fields[4];
                state.put(id, new CrashWorkerMain.AccountRow(id, code, balance, note));
            } else if (op.startsWith("DEL:")) {
                int id = Integer.parseInt(op.substring("DEL:".length()));
                state.remove(id);
            }
        }
    }
}
