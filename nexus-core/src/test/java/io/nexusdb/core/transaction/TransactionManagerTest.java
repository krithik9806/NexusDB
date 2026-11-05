package io.nexusdb.core.transaction;

import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.index.BPlusTree;
import io.nexusdb.core.index.DuplicateKeyException;
import io.nexusdb.core.parser.Parser;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.core.wal.LogReader;
import io.nexusdb.core.wal.LogRecord;
import io.nexusdb.core.wal.LogRecordType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionManagerTest {

    @TempDir
    Path tempDir;

    @Test
    void parserAndAstPrinterRoundTripTransactionStatements() {
        for (String sql : List.of("BEGIN", "COMMIT", "ROLLBACK")) {
            Statement stmt = Parser.parse(sql);
            assertEquals(sql, AstPrinter.print(stmt));
            assertEquals(stmt, Parser.parse(AstPrinter.print(stmt)));
        }
    }

    @Test
    void rollbackRestoresTableHeapAndBPlusTreeIndexesAndWritesClrs() {
        Path dbFile = tempDir.resolve("txn_rollback.db");

        try (Database db = Database.open(dbFile, 32)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        email VARCHAR(64) NOT NULL,
                        tier INT NOT NULL,
                        bio VARCHAR(240) NOT NULL
                    );
                    CREATE UNIQUE INDEX idx_accounts_email ON accounts (email);
                    CREATE INDEX idx_accounts_tier ON accounts (tier);
                    """);

            // Seed committed baseline rows
            for (int i = 1; i <= 20; i++) {
                db.execute(String.format(
                        "INSERT INTO accounts VALUES (%d, 'user%d@nexus.io', %d, 'initial_bio_%d');",
                        i, i, i % 4, i));
            }

            // Begin explicit transaction performing inserts, in-page updates, cross-page relocating updates, and deletes
            db.execute("BEGIN;");
            db.execute("INSERT INTO accounts VALUES (21, 'user21@nexus.io', 1, 'new_user_21');");
            db.execute("INSERT INTO accounts VALUES (22, 'user22@nexus.io', 2, 'new_user_22');");
            db.execute("UPDATE accounts SET email = 'updated1@nexus.io', tier = 3 WHERE id = 1;");
            String hugeBio = "Z".repeat(220);
            db.execute("UPDATE accounts SET bio = '" + hugeBio + "', tier = 0 WHERE id <= 10;");
            db.execute("DELETE FROM accounts WHERE id >= 15 AND id <= 18;");

            // Verify uncommitted changes are visible inside the active transaction
            assertEquals(18, db.execute("SELECT id FROM accounts;").rows().size());

            // Roll back transaction
            db.execute("ROLLBACK;");

            // Verify heap and all indexes match the exact pre-BEGIN baseline (20 rows)
            ResultSet afterRollback = db.execute("SELECT id, email, tier, bio FROM accounts ORDER BY id ASC;");
            assertEquals(20, afterRollback.rows().size());
            for (int i = 1; i <= 20; i++) {
                Tuple row = afterRollback.rows().get(i - 1);
                assertEquals(Value.ofInt(i), row.getValue(0));
                assertEquals(Value.ofVarchar("user" + i + "@nexus.io"), row.getValue(1));
                assertEquals(Value.ofInt(i % 4), row.getValue(2));
                assertEquals(Value.ofVarchar("initial_bio_" + i), row.getValue(3));

                // Point lookup via unique email index
                ResultSet byEmail = db.execute(
                        "SELECT id FROM accounts WHERE email = 'user" + i + "@nexus.io';");
                assertEquals(1, byEmail.rows().size());
                assertEquals(Value.ofInt(i), byEmail.rows().get(0).getValue(0));
            }

            assertTrue(db.execute("SELECT id FROM accounts WHERE email = 'updated1@nexus.io';").rows().isEmpty());
            assertTrue(db.execute("SELECT id FROM accounts WHERE id = 21;").rows().isEmpty());

            TableMetadata table = db.getCatalog().getTable("accounts");
            assertEquals(20L, table.getTableHeap().getLiveRowCount());
            for (IndexMetadata idx : table.getIndexes()) {
                BPlusTree tree = table.getIndexTree(idx);
                tree.verifyInvariants();
            }

            // Verify CLR and ABORT records were written to the WAL
            List<LogRecord> wal = LogReader.readAll(db.getLogManager().getWalPath());
            assertTrue(wal.stream().anyMatch(r -> r.type() == LogRecordType.CLR));
            assertEquals(LogRecordType.ABORT, wal.get(wal.size() - 1).type());
        }
    }

    @Test
    void committedTransactionsAreDurableAcrossReopenAndInterleavedTxnsIsolateRollback() {
        Path dbFile = tempDir.resolve("txn_durable_interleaved.db");

        try (Database db = Database.open(dbFile, 32)) {
            db.execute("""
                    CREATE TABLE ledger (
                        id INT PRIMARY KEY,
                        owner VARCHAR(32) NOT NULL,
                        amount INT NOT NULL
                    );
                    CREATE UNIQUE INDEX idx_ledger_owner ON ledger (owner);
                    """);

            Transaction txn1 = db.beginTransaction();
            Transaction txn2 = db.beginTransaction();

            db.execute(txn1, "INSERT INTO ledger VALUES (1, 'alice', 500);");
            db.execute(txn2, "INSERT INTO ledger VALUES (2, 'bob', 700);");
            db.execute(txn1, "INSERT INTO ledger VALUES (3, 'charlie', 900);");
            db.execute(txn2, "UPDATE ledger SET amount = 750 WHERE id = 2;");

            // Roll back txn1, commit txn2
            db.rollbackTransaction(txn1);
            db.commitTransaction(txn2);

            assertEquals(TransactionState.ABORTED, txn1.getState());
            assertEquals(TransactionState.COMMITTED, txn2.getState());
            assertTrue(db.getLogManager().getFlushedLsn() >= txn2.getLastLsn());

            ResultSet rows = db.execute("SELECT id, owner, amount FROM ledger ORDER BY id ASC;");
            assertEquals(1, rows.rows().size());
            assertEquals(new Tuple(Value.ofInt(2), Value.ofVarchar("bob"), Value.ofInt(750)), rows.rows().get(0));
        }

        // Reopen database and verify committed txn2 is durable and rolled-back txn1 is absent
        try (Database reopened = Database.open(dbFile, 32)) {
            ResultSet rows = reopened.execute("SELECT id, owner, amount FROM ledger ORDER BY id ASC;");
            assertEquals(1, rows.rows().size());
            assertEquals(new Tuple(Value.ofInt(2), Value.ofVarchar("bob"), Value.ofInt(750)), rows.rows().get(0));

            ResultSet byOwner = reopened.execute("SELECT id FROM ledger WHERE owner = 'bob';");
            assertEquals(1, byOwner.rows().size());
            assertTrue(reopened.execute("SELECT id FROM ledger WHERE owner = 'alice';").rows().isEmpty());
        }
    }

    @Test
    void statementLevelAtomicityOnMultiRowInsertAndUpdateConstraintViolation() {
        Path dbFile = tempDir.resolve("txn_stmt_atomicity.db");

        try (Database db = Database.open(dbFile, 32)) {
            db.execute("""
                    CREATE TABLE items (
                        id INT PRIMARY KEY,
                        sku VARCHAR(32) NOT NULL,
                        price INT NOT NULL
                    );
                    CREATE UNIQUE INDEX idx_items_sku ON items (sku);
                    """);

            db.execute("INSERT INTO items VALUES (1, 'SKU-1', 100), (2, 'SKU-2', 200);");

            // Autocommit multi-row INSERT where the 3rd row duplicates primary key id=1
            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("""
                            INSERT INTO items VALUES
                                (3, 'SKU-3', 300),
                                (4, 'SKU-4', 400),
                                (1, 'SKU-DUP', 999);
                            """));

            // Neither (3, 'SKU-3') nor (4, 'SKU-4') may remain in heap or indexes
            assertEquals(2, db.execute("SELECT id FROM items;").rows().size());
            assertTrue(db.execute("SELECT id FROM items WHERE sku = 'SKU-3';").rows().isEmpty());
            assertTrue(db.execute("SELECT id FROM items WHERE sku = 'SKU-4';").rows().isEmpty());

            // Explicit transaction where statement 1 succeeds, statement 2 fails mid-way on unique SKU,
            // and statement 3 succeeds, followed by COMMIT
            db.execute("BEGIN;");
            db.execute("INSERT INTO items VALUES (10, 'SKU-10', 1000);");

            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("""
                            INSERT INTO items VALUES
                                (11, 'SKU-11', 1100),
                                (12, 'SKU-1', 1200);
                            """));

            // Also test multi-row UPDATE failing midway on unique SKU constraint
            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("UPDATE items SET sku = 'SKU-10' WHERE id <= 2;"));

            // Verify id=1 and id=2 still have their original SKUs ('SKU-1' and 'SKU-2')
            assertEquals(
                    Value.ofVarchar("SKU-1"),
                    db.execute("SELECT sku FROM items WHERE id = 1;").rows().get(0).getValue(0));
            assertEquals(
                    Value.ofVarchar("SKU-2"),
                    db.execute("SELECT sku FROM items WHERE id = 2;").rows().get(0).getValue(0));

            db.execute("COMMIT;");

            ResultSet finalRows = db.execute("SELECT id, sku FROM items ORDER BY id ASC;");
            assertEquals(3, finalRows.rows().size());
            assertEquals(Value.ofInt(1), finalRows.rows().get(0).getValue(0));
            assertEquals(Value.ofInt(2), finalRows.rows().get(1).getValue(0));
            assertEquals(Value.ofInt(10), finalRows.rows().get(2).getValue(0));
            assertFalse(finalRows.rows().stream().anyMatch(t -> t.getValue(0).equals(Value.ofInt(11))));
        }
    }
}
