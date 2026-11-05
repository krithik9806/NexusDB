package io.nexusdb.core.transaction;

import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.index.DuplicateKeyException;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConcurrencySuiteTest {

    @TempDir
    Path tempDir;

    @Test
    void hundredThreadsRandomBankTransfersPreserveTotalBalanceInvariant() throws Exception {
        Path dbFile = tempDir.resolve("bank_100_threads.db");
        final int accountCount = 50;
        final int initialBalance = 1_000;
        final int expectedTotal = accountCount * initialBalance;
        final int threadCount = 100;
        final int transfersPerThread = 5;

        try (Database db = Database.open(dbFile, 32)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        tier INT NOT NULL,
                        balance INT NOT NULL
                    );
                    CREATE INDEX idx_accounts_tier ON accounts (tier);
                    """);

            for (int i = 1; i <= accountCount; i++) {
                db.execute(String.format(
                        "INSERT INTO accounts VALUES (%d, %d, %d);",
                        i, i % 5, initialBalance));
            }

            CountDownLatch startLatch = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            try {
                List<Future<Void>> futures = new ArrayList<>(threadCount);
                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t;
                    futures.add(pool.submit(() -> {
                        Random rng = new Random(10_000L + threadId * 97L);
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));

                        for (int step = 0; step < transfersPerThread; step++) {
                            int fromId = 1 + rng.nextInt(accountCount);
                            int toId = 1 + rng.nextInt(accountCount - 1);
                            if (toId >= fromId) {
                                toId++;
                            }
                            int amount = 1 + rng.nextInt(40);
                            executeTransferWithRetry(db, fromId, toId, amount, rng);
                        }
                        return null;
                    }));
                }

                startLatch.countDown();
                for (Future<Void> f : futures) {
                    f.get(60, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertTotalBalanceAndIndexInvariants(db, accountCount, expectedTotal);

            TransactionMetrics metrics = db.getTransactionMetrics();
            assertEquals(0, metrics.activeTransactions());
            assertTrue(metrics.committedCount() >= accountCount + (long) threadCount * transfersPerThread);
        }
    }

    @Test
    void lostUpdatePreventionUnderConcurrentReadModifyWriteTransactions() throws Exception {
        Path dbFile = tempDir.resolve("lost_update.db");
        final int workers = 32;
        final int incrementsPerWorker = 4;
        final int expectedFinalCounter = workers * incrementsPerWorker;

        try (Database db = Database.open(dbFile, 16)) {
            db.execute("""
                    CREATE TABLE counters (
                        id INT PRIMARY KEY,
                        val INT NOT NULL
                    );
                    INSERT INTO counters VALUES (1, 0);
                    """);

            CountDownLatch startLatch = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            try {
                List<Future<Void>> futures = new ArrayList<>(workers);
                for (int w = 0; w < workers; w++) {
                    final int workerId = w;
                    futures.add(pool.submit(() -> {
                        Random rng = new Random(777L + workerId);
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));

                        for (int i = 0; i < incrementsPerWorker; i++) {
                            while (true) {
                                Transaction txn = db.beginTransaction();
                                try {
                                    // Read current value (acquires SHARED lock on id=1)
                                    ResultSet rs = db.execute(txn, "SELECT val FROM counters WHERE id = 1;");
                                    int currentVal = ((Value.IntValue) rs.rows().get(0).getValue(0)).value();

                                    // Write incremented value (upgrades SHARED -> EXCLUSIVE lock on id=1)
                                    db.execute(
                                            txn,
                                            "UPDATE counters SET val = " + (currentVal + 1) + " WHERE id = 1;");
                                    db.commitTransaction(txn);
                                    break;
                                } catch (DeadlockException de) {
                                    // Victim transaction is automatically rolled back; retry after brief jitter
                                    Thread.sleep(1 + rng.nextInt(3));
                                }
                            }
                        }
                        return null;
                    }));
                }

                startLatch.countDown();
                for (Future<Void> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            ResultSet finalRs = db.execute("SELECT val FROM counters WHERE id = 1;");
            int actualFinalVal = ((Value.IntValue) finalRs.rows().get(0).getValue(0)).value();
            assertEquals(expectedFinalCounter, actualFinalVal, "Zero lost updates may occur under Strict 2PL");
            assertEquals(0, db.getTransactionMetrics().activeTransactions());
        }
    }

    @Test
    void concurrentInsertsWithPrimaryKeyConflictsAllowExactlyOneWinner() throws Exception {
        Path dbFile = tempDir.resolve("pk_conflict.db");
        final int contenders = 40;

        try (Database db = Database.open(dbFile, 16)) {
            db.execute("""
                    CREATE TABLE users (
                        id INT PRIMARY KEY,
                        username VARCHAR(64) NOT NULL,
                        score INT NOT NULL
                    );
                    CREATE UNIQUE INDEX idx_users_username ON users (username);
                    """);

            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger duplicateFailures = new AtomicInteger();
            AtomicInteger winningThread = new AtomicInteger(-1);

            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            try {
                List<Future<Void>> futures = new ArrayList<>(contenders);
                for (int i = 0; i < contenders; i++) {
                    final int threadIdx = i;
                    futures.add(pool.submit(() -> {
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));
                        Transaction txn = db.beginTransaction();
                        try {
                            db.execute(
                                    txn,
                                    String.format(
                                            "INSERT INTO users VALUES (42, 'contender_%d', %d);",
                                            threadIdx, threadIdx * 10));
                            db.commitTransaction(txn);
                            successes.incrementAndGet();
                            winningThread.set(threadIdx);
                        } catch (DuplicateKeyException dup) {
                            db.rollbackTransaction(txn);
                            duplicateFailures.incrementAndGet();
                        } catch (DeadlockException de) {
                            duplicateFailures.incrementAndGet();
                        }
                        return null;
                    }));
                }

                startLatch.countDown();
                for (Future<Void> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertEquals(1, successes.get(), "Exactly one thread must succeed on conflicting PK insert");
            assertEquals(contenders - 1, duplicateFailures.get());

            ResultSet rows = db.execute("SELECT id, username, score FROM users;");
            assertEquals(1, rows.rows().size());
            Tuple onlyRow = rows.rows().get(0);
            assertEquals(Value.ofInt(42), onlyRow.getValue(0));
            assertEquals(Value.ofVarchar("contender_" + winningThread.get()), onlyRow.getValue(1));

            TableMetadata table = db.getCatalog().getTable("users");
            assertEquals(1L, table.getTableHeap().getLiveRowCount());
            for (IndexMetadata idx : table.getIndexes()) {
                table.getIndexTree(idx).verifyInvariants();
            }
        }
    }

    @Test
    void concurrentReadersAndWritersObserveConsistentSnapshotsAndDeadlockStormRecovers() throws Exception {
        Path dbFile = tempDir.resolve("readers_writers_deadlock_storm.db");
        final int accountCount = 10;
        final int initialBalance = 1_000;
        final int expectedTotal = accountCount * initialBalance;

        try (Database db = Database.open(dbFile, 16)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        balance INT NOT NULL
                    );
                    """);
            for (int i = 1; i <= accountCount; i++) {
                db.execute("INSERT INTO accounts VALUES (" + i + ", " + initialBalance + ");");
            }

            int writerThreads = 20;
            int readerThreads = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger completedWriters = new AtomicInteger();
            ExecutorService pool = Executors.newFixedThreadPool(writerThreads + readerThreads);
            try {
                List<Future<Void>> futures = new ArrayList<>();

                // Writers intentionally lock accounts in OPPOSITE orders (even vs odd threads)
                // to trigger a heavy deadlock storm while retrying until every transfer commits.
                for (int w = 0; w < writerThreads; w++) {
                    final int writerId = w;
                    futures.add(pool.submit(() -> {
                        Random rng = new Random(9_000L + writerId * 53L);
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));

                        for (int step = 0; step < 6; step++) {
                            int a = 1 + (step % (accountCount - 1));
                            int b = a + 1;
                            int first = (writerId % 2 == 0) ? a : b;
                            int second = (writerId % 2 == 0) ? b : a;
                            executeTransferInExactOrderWithRetry(db, first, second, 10, rng);
                        }
                        completedWriters.incrementAndGet();
                        return null;
                    }));
                }

                // Concurrent readers continuously scan the entire table and verify SUM(balance) == expectedTotal
                for (int r = 0; r < readerThreads; r++) {
                    final int readerId = r;
                    futures.add(pool.submit(() -> {
                        Random rng = new Random(19_000L + readerId);
                        assertTrue(startLatch.await(10, TimeUnit.SECONDS));

                        while (completedWriters.get() < writerThreads) {
                            while (true) {
                                Transaction readTxn = db.beginTransaction();
                                try {
                                    ResultSet rs = db.execute(readTxn, "SELECT id, balance FROM accounts;");
                                    assertEquals(accountCount, rs.rows().size());
                                    int sum = 0;
                                    for (Tuple row : rs.rows()) {
                                        sum += ((Value.IntValue) row.getValue(1)).value();
                                    }
                                    assertEquals(
                                            expectedTotal,
                                            sum,
                                            "Concurrent reader observed inconsistent mid-transfer total balance");
                                    db.commitTransaction(readTxn);
                                    break;
                                } catch (DeadlockException de) {
                                    Thread.sleep(1 + rng.nextInt(3));
                                }
                            }
                            Thread.sleep(2);
                        }
                        return null;
                    }));
                }

                startLatch.countDown();
                for (Future<Void> f : futures) {
                    f.get(60, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            TransactionMetrics metrics = db.getTransactionMetrics();
            assertEquals(0, metrics.activeTransactions());
            assertTrue(metrics.deadlockCount() > 0L, "Deadlock storm must trigger and resolve deadlocks");
            assertTrue(metrics.abortedCount() >= metrics.deadlockCount());
            assertTotalBalanceAndIndexInvariants(db, accountCount, expectedTotal);
        }
    }

    @Test
    void fiftyIterationConcurrencyStressLoopOnConstrainedBufferPool() throws Exception {
        Path dbFile = tempDir.resolve("stress_50_iterations.db");
        final int accountCount = 20;
        final int initialBalance = 500;
        final int expectedTotal = accountCount * initialBalance;

        // Use a constrained 16-page buffer pool to force continuous frame evictions under 16-thread concurrency
        try (Database db = Database.open(dbFile, 16)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        tier INT NOT NULL,
                        balance INT NOT NULL
                    );
                    CREATE INDEX idx_accounts_tier ON accounts (tier);
                    """);
            for (int i = 1; i <= accountCount; i++) {
                db.execute(String.format(
                        "INSERT INTO accounts VALUES (%d, %d, %d);",
                        i, i % 4, initialBalance));
            }

            ExecutorService pool = Executors.newFixedThreadPool(16);
            try {
                for (int iteration = 0; iteration < 50; iteration++) {
                    final int iter = iteration;
                    List<Callable<Void>> tasks = new ArrayList<>(16);
                    for (int t = 0; t < 16; t++) {
                        final int threadId = t;
                        tasks.add(() -> {
                            Random rng = new Random(100_000L + iter * 1_009L + threadId * 37L);
                            int fromId = 1 + rng.nextInt(accountCount);
                            int toId = 1 + rng.nextInt(accountCount - 1);
                            if (toId >= fromId) {
                                toId++;
                            }
                            int amount = 1 + rng.nextInt(25);
                            executeTransferInExactOrderWithRetry(db, fromId, toId, amount, rng);
                            return null;
                        });
                    }
                    List<Future<Void>> futures = pool.invokeAll(tasks);
                    for (Future<Void> f : futures) {
                        f.get(30, TimeUnit.SECONDS);
                    }
                    assertTotalBalanceAndIndexInvariants(db, accountCount, expectedTotal);
                }
            } finally {
                pool.shutdownNow();
            }

            TransactionMetrics metrics = db.getTransactionMetrics();
            assertEquals(0, metrics.activeTransactions());
            assertTrue(metrics.committedCount() >= 50L * 16L);
        }
    }

    private static void executeTransferWithRetry(
            Database db,
            int fromId,
            int toId,
            int amount,
            Random rng) throws InterruptedException {
        int firstId = Math.min(fromId, toId);
        int secondId = Math.max(fromId, toId);
        while (true) {
            Transaction txn = db.beginTransaction();
            try {
                ResultSet r1 = db.execute(txn, "SELECT balance FROM accounts WHERE id = " + firstId + ";");
                ResultSet r2 = db.execute(txn, "SELECT balance FROM accounts WHERE id = " + secondId + ";");
                int bal1 = ((Value.IntValue) r1.rows().get(0).getValue(0)).value();
                int bal2 = ((Value.IntValue) r2.rows().get(0).getValue(0)).value();

                int newFromBal = (fromId == firstId ? bal1 : bal2) - amount;
                int newToBal = (toId == firstId ? bal1 : bal2) + amount;

                db.execute(txn, "UPDATE accounts SET balance = " + newFromBal + " WHERE id = " + fromId + ";");
                db.execute(txn, "UPDATE accounts SET balance = " + newToBal + " WHERE id = " + toId + ";");
                db.commitTransaction(txn);
                return;
            } catch (DeadlockException de) {
                Thread.sleep(1 + rng.nextInt(4));
            }
        }
    }

    private static void executeTransferInExactOrderWithRetry(
            Database db,
            int fromId,
            int toId,
            int amount,
            Random rng) throws InterruptedException {
        while (true) {
            Transaction txn = db.beginTransaction();
            try {
                ResultSet rFrom = db.execute(txn, "SELECT balance FROM accounts WHERE id = " + fromId + ";");
                int fromBal = ((Value.IntValue) rFrom.rows().get(0).getValue(0)).value();
                db.execute(
                        txn,
                        "UPDATE accounts SET balance = " + (fromBal - amount) + " WHERE id = " + fromId + ";");

                ResultSet rTo = db.execute(txn, "SELECT balance FROM accounts WHERE id = " + toId + ";");
                int toBal = ((Value.IntValue) rTo.rows().get(0).getValue(0)).value();
                db.execute(
                        txn,
                        "UPDATE accounts SET balance = " + (toBal + amount) + " WHERE id = " + toId + ";");

                db.commitTransaction(txn);
                return;
            } catch (DeadlockException de) {
                Thread.sleep(1 + rng.nextInt(4));
            }
        }
    }

    private static void assertTotalBalanceAndIndexInvariants(
            Database db,
            int expectedRows,
            int expectedTotalBalance) {
        ResultSet rows = db.execute("SELECT id, balance FROM accounts ORDER BY id ASC;");
        assertEquals(expectedRows, rows.rows().size());
        int total = 0;
        for (Tuple row : rows.rows()) {
            total += ((Value.IntValue) row.getValue(1)).value();
        }
        assertEquals(expectedTotalBalance, total, "Total bank balance invariant violated");

        TableMetadata table = db.getCatalog().getTable("accounts");
        assertEquals((long) expectedRows, table.getTableHeap().getLiveRowCount());
        for (IndexMetadata idx : table.getIndexes()) {
            table.getIndexTree(idx).verifyInvariants();
        }
    }
}
