package io.nexusdb.core.transaction;

import io.nexusdb.core.engine.Database;
import io.nexusdb.core.page.RecordId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void lockCompatibilityMatrixSharedAndExclusive() throws Exception {
        try (LockManager lm = new LockManager(1_000L, 10L)) {
            LockId row1 = LockId.row("accounts", new RecordId(1, 0));

            // Multiple transactions can hold SHARED locks concurrently
            lm.acquire(1L, row1, LockMode.SHARED);
            lm.acquire(2L, row1, LockMode.SHARED);
            lm.acquire(3L, row1, LockMode.SHARED);
            assertTrue(lm.holdsLock(1L, row1, LockMode.SHARED));
            assertTrue(lm.holdsLock(2L, row1, LockMode.SHARED));
            assertTrue(lm.holdsLock(3L, row1, LockMode.SHARED));
            assertFalse(lm.holdsLock(1L, row1, LockMode.EXCLUSIVE));

            // Re-entrant SHARED request is a no-op
            lm.acquire(1L, row1, LockMode.SHARED);

            // EXCLUSIVE request by another transaction is incompatible with SHARED and must block/timeout
            assertThrows(
                    LockTimeoutException.class,
                    () -> lm.acquire(4L, row1, LockMode.EXCLUSIVE, 40L));

            // Release all SHARED holders; now txn 4 can acquire EXCLUSIVE
            lm.releaseAll(1L);
            lm.releaseAll(2L);
            lm.releaseAll(3L);

            lm.acquire(4L, row1, LockMode.EXCLUSIVE);
            assertTrue(lm.holdsLock(4L, row1, LockMode.EXCLUSIVE));
            assertTrue(lm.holdsLock(4L, row1, LockMode.SHARED));

            // Neither SHARED nor EXCLUSIVE by another transaction is compatible with EXCLUSIVE
            assertThrows(
                    LockTimeoutException.class,
                    () -> lm.acquire(5L, row1, LockMode.SHARED, 40L));
            assertThrows(
                    LockTimeoutException.class,
                    () -> lm.acquire(6L, row1, LockMode.EXCLUSIVE, 40L));

            lm.releaseAll(4L);
            assertFalse(lm.holdsLock(4L, row1, LockMode.SHARED));
        }
    }

    @Test
    void lockUpgradeSoleHolderImmediateAndContendedHolderPrioritized() throws Exception {
        try (LockManager lm = new LockManager(2_000L, 15L)) {
            LockId row = LockId.row("accounts", new RecordId(2, 5));

            // 1. Sole SHARED holder upgrades immediately in-place to EXCLUSIVE
            lm.acquire(10L, row, LockMode.SHARED);
            assertTrue(lm.holdsLock(10L, row, LockMode.SHARED));
            assertFalse(lm.holdsLock(10L, row, LockMode.EXCLUSIVE));

            lm.acquire(10L, row, LockMode.EXCLUSIVE);
            assertTrue(lm.holdsLock(10L, row, LockMode.EXCLUSIVE));
            lm.releaseAll(10L);

            // 2. Contended upgrade: txn 20 and txn 21 hold SHARED; txn 20 requests upgrade to EXCLUSIVE,
            //    while txn 22 queues a new EXCLUSIVE request behind it. When txn 21 releases SHARED,
            //    txn 20's upgrade must be granted BEFORE txn 22's queued EXCLUSIVE request.
            lm.acquire(20L, row, LockMode.SHARED);
            lm.acquire(21L, row, LockMode.SHARED);

            List<Long> grantOrder = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch upgradeStarted = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> upgrader = pool.submit(() -> {
                    upgradeStarted.countDown();
                    lm.acquire(20L, row, LockMode.EXCLUSIVE);
                    grantOrder.add(20L);
                    lm.releaseAll(20L);
                });

                assertTrue(upgradeStarted.await(2, TimeUnit.SECONDS));
                while (lm.getCurrentWaitingCount() < 1) {
                    Thread.sleep(5);
                }

                Future<?> newcomer = pool.submit(() -> {
                    lm.acquire(22L, row, LockMode.EXCLUSIVE);
                    grantOrder.add(22L);
                    lm.releaseAll(22L);
                });

                while (lm.getCurrentWaitingCount() < 2) {
                    Thread.sleep(5);
                }

                // Release txn 21's SHARED lock; upgrade (20L) must win before newcomer (22L)
                lm.releaseAll(21L);

                upgrader.get(3, TimeUnit.SECONDS);
                newcomer.get(3, TimeUnit.SECONDS);
                assertEquals(List.of(20L, 22L), grantOrder);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void twoTransactionDeadlockDetectedAndYoungestAborted() throws Exception {
        try (LockManager lm = new LockManager(3_000L, 10L)) {
            LockId resA = LockId.row("accounts", new RecordId(1, 1));
            LockId resB = LockId.row("accounts", new RecordId(1, 2));

            // Older txnId=1 holds resA; younger txnId=2 holds resB
            lm.acquire(1L, resA, LockMode.EXCLUSIVE);
            lm.acquire(2L, resB, LockMode.EXCLUSIVE);

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                // txn 1 waits for resB; txn 2 waits for resA -> cycle 1 <-> 2
                Future<?> f1 = pool.submit(() -> {
                    lm.acquire(1L, resB, LockMode.EXCLUSIVE);
                    lm.releaseAll(1L);
                });
                Future<?> f2 = pool.submit(() -> {
                    lm.acquire(2L, resA, LockMode.EXCLUSIVE);
                    lm.releaseAll(2L);
                });

                // Youngest transaction (txnId=2) must abort with DeadlockException
                ExecutionException ex = assertThrows(ExecutionException.class, () -> f2.get(3, TimeUnit.SECONDS));
                assertTrue(ex.getCause() instanceof DeadlockException);
                DeadlockException de = (DeadlockException) ex.getCause();
                assertEquals(2L, de.getVictimTxnId());
                assertTrue(de.getMessage().contains("txnId=2"));

                // Older transaction (txnId=1) must succeed once victim txnId=2 is aborted
                f1.get(3, TimeUnit.SECONDS);
                assertTrue(lm.getDeadlockCount() >= 1L);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void threeTransactionDeadlockCycleDetectedAndYoungestAborted() throws Exception {
        try (LockManager lm = new LockManager(3_000L, 10L)) {
            LockId r1 = LockId.table("t1");
            LockId r2 = LockId.table("t2");
            LockId r3 = LockId.table("t3");

            // txn 10 holds r1, txn 20 holds r2, txn 30 (youngest) holds r3
            lm.acquire(10L, r1, LockMode.EXCLUSIVE);
            lm.acquire(20L, r2, LockMode.EXCLUSIVE);
            lm.acquire(30L, r3, LockMode.EXCLUSIVE);

            ExecutorService pool = Executors.newFixedThreadPool(3);
            try {
                // 3-way cycle: 10 -> 20 -> 30 -> 10
                Future<?> f10 = pool.submit(() -> {
                    lm.acquire(10L, r2, LockMode.EXCLUSIVE);
                    lm.releaseAll(10L);
                });
                Future<?> f20 = pool.submit(() -> {
                    lm.acquire(20L, r3, LockMode.EXCLUSIVE);
                    lm.releaseAll(20L);
                });
                Future<?> f30 = pool.submit(() -> {
                    lm.acquire(30L, r1, LockMode.EXCLUSIVE);
                    lm.releaseAll(30L);
                });

                // Youngest transaction (txnId=30) must be aborted
                ExecutionException ex = assertThrows(ExecutionException.class, () -> f30.get(3, TimeUnit.SECONDS));
                assertTrue(ex.getCause() instanceof DeadlockException);
                DeadlockException de = (DeadlockException) ex.getCause();
                assertEquals(30L, de.getVictimTxnId());

                // Both older transactions (20 and 10) must complete cleanly
                f20.get(3, TimeUnit.SECONDS);
                f10.get(3, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void fifoFairnessPreventsReaderStarvationOfWaitingWriter() throws Exception {
        try (LockManager lm = new LockManager(3_000L, 15L)) {
            LockId row = LockId.row("accounts", new RecordId(3, 0));

            // Txn 1 holds SHARED
            lm.acquire(1L, row, LockMode.SHARED);

            List<Long> acquisitionOrder = Collections.synchronizedList(new ArrayList<>());
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                // Txn 2 queues EXCLUSIVE behind Txn 1
                Future<?> writer2 = pool.submit(() -> {
                    lm.acquire(2L, row, LockMode.EXCLUSIVE);
                    acquisitionOrder.add(2L);
                    lm.releaseAll(2L);
                });

                while (lm.getCurrentWaitingCount() < 1) {
                    Thread.sleep(5);
                }

                // Txn 3 requests SHARED after Txn 2 is already waiting for EXCLUSIVE;
                // FIFO fairness must prevent Txn 3 from jumping ahead of waiting writer Txn 2.
                Future<?> reader3 = pool.submit(() -> {
                    lm.acquire(3L, row, LockMode.SHARED);
                    acquisitionOrder.add(3L);
                    lm.releaseAll(3L);
                });

                while (lm.getCurrentWaitingCount() < 2) {
                    Thread.sleep(5);
                }

                // Release Txn 1; Txn 2 (writer) must acquire before Txn 3 (reader)
                lm.releaseAll(1L);

                writer2.get(3, TimeUnit.SECONDS);
                reader3.get(3, TimeUnit.SECONDS);
                assertEquals(List.of(2L, 3L), acquisitionOrder);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void databaseExecutorsEnforceStrict2plAndRollbackDeadlockVictim() throws Exception {
        Path dbFile = tempDir.resolve("strict2pl.db");
        try (Database db = Database.open(dbFile, 32)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        balance INT NOT NULL
                    );
                    INSERT INTO accounts VALUES (1, 1000), (2, 2000);
                    """);

            Transaction olderTxn = db.beginTransaction();
            Transaction youngerTxn = db.beginTransaction();

            // olderTxn updates id=1 (acquires X lock on id=1)
            db.execute(olderTxn, "UPDATE accounts SET balance = 1100 WHERE id = 1;");
            // youngerTxn updates id=2 (acquires X lock on id=2)
            db.execute(youngerTxn, "UPDATE accounts SET balance = 2200 WHERE id = 2;");

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                // olderTxn reads id=2 (requests S lock on id=2, held in X mode by youngerTxn)
                Future<?> olderFuture = pool.submit(() -> {
                    db.execute(olderTxn, "SELECT balance FROM accounts WHERE id = 2;");
                });

                while (db.getLockManager().getCurrentWaitingCount() < 1) {
                    Thread.sleep(5);
                }

                // youngerTxn updates id=1 (requests X lock on id=1, held in X mode by olderTxn) -> Deadlock!
                Future<?> youngerFuture = pool.submit(() -> {
                    db.execute(youngerTxn, "UPDATE accounts SET balance = 9999 WHERE id = 1;");
                });

                ExecutionException ex = assertThrows(
                        ExecutionException.class,
                        () -> youngerFuture.get(3, TimeUnit.SECONDS));
                assertTrue(ex.getCause() instanceof DeadlockException);
                assertEquals(TransactionState.ABORTED, youngerTxn.getState());

                // olderTxn unblocks, sees rolled-back balance=2000 on id=2, and commits!
                olderFuture.get(3, TimeUnit.SECONDS);
                db.commitTransaction(olderTxn);

                var rows = db.execute("SELECT id, balance FROM accounts ORDER BY id ASC;").rows();
                assertEquals(1100, (( io.nexusdb.core.record.Value.IntValue) rows.get(0).getValue(1)).value());
                assertEquals(2000, (( io.nexusdb.core.record.Value.IntValue) rows.get(1).getValue(1)).value());
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
