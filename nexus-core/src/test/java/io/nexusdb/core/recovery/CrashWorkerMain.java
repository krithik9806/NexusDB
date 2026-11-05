package io.nexusdb.core.recovery;

import io.nexusdb.core.engine.Database;
import io.nexusdb.core.transaction.Transaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Child JVM process worker for {@link CrashRecoveryHarnessTest}.
 *
 * <p>Executes a continuous transactional workload (inserts, in-page and relocating updates, deletes,
 * explicit rollbacks, fuzzy checkpoints, and uncommitted loser transactions) against a database file
 * while logging ground-truth committed state transitions to an external fsynced journal file.
 */
public final class CrashWorkerMain {

    private CrashWorkerMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            throw new IllegalArgumentException(
                    "Usage: CrashWorkerMain <dbFile> <truthLog> <seed> <failpoint> <failpointAfter>");
        }
        Path dbFile = Path.of(args[0]);
        Path truthLog = Path.of(args[1]);
        long seed = Long.parseLong(args[2]);
        String modeOrFailpoint = args[3];
        int failpointAfter = Integer.parseInt(args[4]);

        if ("RECOVER_WITH_UNDO_FAILPOINT".equals(modeOrFailpoint)) {
            CrashFailpoints.configure(CrashFailpoints.Point.DURING_UNDO_RECOVERY, failpointAfter);
            try (Database ignored = Database.open(dbFile, 16)) {
                // Should halt mid-Undo if failpoint triggers
            }
            return;
        }

        CrashFailpoints.Point failpoint = CrashFailpoints.Point.valueOf(modeOrFailpoint);

        try (FileChannel truthChannel = FileChannel.open(
                truthLog,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
             Database db = Database.open(dbFile, 16)) {

            if (!db.getCatalog().hasTable("accounts")) {
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
                db.getBufferPoolManager().flushAll();
            }

            // Signal to parent harness that schema initialization is complete
            System.out.println("READY");
            System.out.flush();

            Random rng = new Random(seed);
            Map<Integer, AccountRow> committedState = new LinkedHashMap<>();
            int nextId = 1;
            int step = 0;

            while (true) {
                step++;
                // Arm failpoint only after baseline committed transactions have populated state
                if (step == 4 && failpoint != CrashFailpoints.Point.NONE) {
                    CrashFailpoints.configure(failpoint, failpointAfter);
                }

                // Periodically take a fuzzy checkpoint while dirty pages exist in the buffer pool
                if (step % 5 == 0) {
                    db.checkpoint();
                }

                // Decide whether this transaction will COMMIT (70%) or ROLLBACK / remain uncommitted (30%)
                boolean willCommit = (step <= 3) || (rng.nextInt(10) < 7);

                if (willCommit) {
                    Transaction txn = db.beginTransaction();
                    long txnId = txn.getTxnId();
                    List<String> opLogEntries = new ArrayList<>();
                    Map<Integer, AccountRow> staged = new LinkedHashMap<>(committedState);

                    int opsInTxn = 1 + rng.nextInt(3);
                    for (int op = 0; op < opsInTxn; op++) {
                        int action = staged.isEmpty() ? 0 : rng.nextInt(10);
                        if (action < 5 || staged.size() < 4) {
                            int id = nextId++;
                            String code = "ACC_" + id + "_T" + txnId;
                            int balance = 100 + rng.nextInt(9000);
                            String note = "committed_txn_" + txnId + "_step_" + step;
                            db.execute(txn, String.format(
                                    "INSERT INTO accounts VALUES (%d, '%s', %d, '%s');",
                                    id, code, balance, note));
                            AccountRow row = new AccountRow(id, code, balance, note);
                            staged.put(id, row);
                            opLogEntries.add(row.encodeUpsert());
                        } else if (action < 8) {
                            List<Integer> ids = new ArrayList<>(staged.keySet());
                            int targetId = ids.get(rng.nextInt(ids.size()));
                            String newCode = "ACC_" + targetId + "_U" + txnId + "_" + op;
                            int newBalance = 500 + rng.nextInt(9000);
                            String newNote = (op % 2 == 0)
                                    ? "expanded_" + "X".repeat(90) + "_txn_" + txnId
                                    : "short_txn_" + txnId;
                            db.execute(txn, String.format(
                                    "UPDATE accounts SET code = '%s', balance = %d, note = '%s' WHERE id = %d;",
                                    newCode, newBalance, newNote, targetId));
                            AccountRow updated = new AccountRow(targetId, newCode, newBalance, newNote);
                            staged.put(targetId, updated);
                            opLogEntries.add(updated.encodeUpsert());
                        } else {
                            List<Integer> ids = new ArrayList<>(staged.keySet());
                            int targetId = ids.get(rng.nextInt(ids.size()));
                            db.execute(txn, "DELETE FROM accounts WHERE id = " + targetId + ";");
                            staged.remove(targetId);
                            opLogEntries.add("DEL:" + targetId);
                        }
                    }

                    writeTruthLine(truthChannel, "CANDIDATE|" + txnId + "|" + String.join(";", opLogEntries));
                    db.commitTransaction(txn);
                    writeTruthLine(truthChannel, "CONFIRMED|" + txnId);
                    committedState = staged;
                } else {
                    // Execute an uncommitted or rolled-back transaction whose mutations MUST NOT survive recovery
                    Transaction loserTxn = db.beginTransaction();
                    int tempId1 = 100_000 + step * 2;
                    int tempId2 = 100_000 + step * 2 + 1;
                    db.execute(loserTxn, String.format(
                            "INSERT INTO accounts VALUES (%d, 'UNCOMMITTED_%d', 999999, 'loser_row_1');",
                            tempId1, tempId1));
                    db.execute(loserTxn, String.format(
                            "INSERT INTO accounts VALUES (%d, 'UNCOMMITTED_%d', 888888, 'loser_row_2');",
                            tempId2, tempId2));
                    if (!committedState.isEmpty()) {
                        int existingId = committedState.keySet().iterator().next();
                        db.execute(loserTxn, String.format(
                                "UPDATE accounts SET balance = -777777, note = 'CORRUPTED_BY_LOSER' WHERE id = %d;",
                                existingId));
                    }

                    if (failpoint != CrashFailpoints.Point.NONE && step >= 6) {
                        // Flush dirty pages of this loser transaction to exercise STEAL + UNDO recovery
                        db.getBufferPoolManager().flushAll();
                        Runtime.getRuntime().halt(99);
                    }

                    // Otherwise roll back explicitly after a brief window so external kills can catch it active
                    Thread.sleep(2);
                    db.rollbackTransaction(loserTxn);
                }
            }
        }
    }

    private static void writeTruthLine(FileChannel channel, String line) throws IOException {
        byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        channel.force(false);
    }

    public record AccountRow(int id, String code, int balance, String note) {
        public String encodeUpsert() {
            return "UPSERT:" + id + ":" + code + ":" + balance + ":" + note;
        }
    }
}
