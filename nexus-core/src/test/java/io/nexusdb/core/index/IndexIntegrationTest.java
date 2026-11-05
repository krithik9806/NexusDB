package io.nexusdb.core.index;

import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.executor.IndexScanExecutor;
import io.nexusdb.core.parser.Parser;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void parserAndAstPrinterRoundTripCreateAndDropIndex() {
        String[] sqlStatements = {
                "CREATE INDEX idx_users_age ON users (age)",
                "CREATE UNIQUE INDEX idx_users_email ON users (email)",
                "DROP INDEX idx_users_age",
                "DROP INDEX idx_users_email ON users"
        };
        for (String sql : sqlStatements) {
            Statement parsed = Parser.parse(sql);
            String printed = AstPrinter.print(parsed);
            assertEquals(sql, printed);
            assertEquals(parsed, Parser.parse(printed));
        }
    }

    @Test
    void primaryKeyEnforcesUniquenessAndCreateIndexBackfillsExistingRows() {
        Path dbFile = tempDir.resolve("idx_pk_backfill.db");

        try (Database db = Database.open(dbFile)) {
            db.execute("""
                    CREATE TABLE accounts (
                        id INT PRIMARY KEY,
                        username VARCHAR(40) NOT NULL,
                        tier INT,
                        notes VARCHAR(250)
                    );
                    """);

            for (int i = 1; i <= 40; i++) {
                db.execute(String.format(
                        "INSERT INTO accounts VALUES (%d, 'user_%03d', %d, 'initial_%d');",
                        i, i, i % 5, i));
            }

            // Primary key duplicate on INSERT must throw DuplicateKeyException and leave row count at 40
            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("INSERT INTO accounts VALUES (15, 'impostor', 1, 'bad');"));
            assertEquals(40, db.execute("SELECT id FROM accounts;").rows().size());

            // Primary key duplicate on UPDATE must throw DuplicateKeyException
            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("UPDATE accounts SET id = 10 WHERE id = 20;"));

            // Create secondary UNIQUE index on username and non-unique index on tier over existing 40 rows
            db.execute("CREATE UNIQUE INDEX idx_accounts_username ON accounts (username);");
            db.execute("CREATE INDEX idx_accounts_tier ON accounts (tier);");

            // Secondary unique index duplicate rejection on INSERT and UPDATE
            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("INSERT INTO accounts VALUES (999, 'user_010', 2, 'dup_user');"));
            assertThrows(
                    DuplicateKeyException.class,
                    () -> db.execute("UPDATE accounts SET username = 'user_005' WHERE id = 6;"));

            // Equality query using secondary unique index
            ResultSet userRs = db.execute("SELECT id, username, tier FROM accounts WHERE username = 'user_025';");
            assertEquals(1, userRs.rows().size());
            assertEquals(Value.ofInt(25), userRs.rows().get(0).getValue(0));

            // Equality query using secondary non-unique index (tier = 3 -> 8 rows out of 40)
            ResultSet tierRs = db.execute("SELECT id FROM accounts WHERE tier = 3 ORDER BY id ASC;");
            assertEquals(8, tierRs.rows().size());
            assertEquals(Value.ofInt(3), tierRs.rows().get(0).getValue(0));
            assertEquals(Value.ofInt(38), tierRs.rows().get(7).getValue(0));

            // Range query on primary key index with residual filter
            ResultSet rangeRs = db.execute(
                    "SELECT id FROM accounts WHERE id >= 10 AND id <= 25 AND tier = 0 ORDER BY id ASC;");
            assertEquals(
                    List.of(
                            new Tuple(Value.ofInt(10)),
                            new Tuple(Value.ofInt(15)),
                            new Tuple(Value.ofInt(20)),
                            new Tuple(Value.ofInt(25))),
                    rangeRs.rows());

            // Force cross-page tuple relocation on TableHeap.update by expanding VARCHAR(250) on early rows
            String longNotes = "X".repeat(240);
            db.execute("UPDATE accounts SET notes = '" + longNotes + "' WHERE id <= 25;");

            // Verify all indexes accurately resolve relocated RecordIds
            for (int i = 1; i <= 25; i++) {
                ResultSet check = db.execute(
                        "SELECT id, notes FROM accounts WHERE username = '" + String.format("user_%03d", i) + "';");
                assertEquals(1, check.rows().size());
                assertEquals(Value.ofInt(i), check.rows().get(0).getValue(0));
                assertEquals(Value.ofVarchar(longNotes), check.rows().get(0).getValue(1));
            }

            // Drop index and verify catalog removal
            db.execute("DROP INDEX idx_accounts_tier;");
            assertNull(db.getCatalog().findIndex("idx_accounts_tier"));
            assertEquals(8, db.execute("SELECT id FROM accounts WHERE tier = 3;").rows().size());
        }
    }

    @Test
    void randomDmlMaintainsIndexConsistencyWithHeapAndPersistsAcrossReopen() {
        Path dbFile = tempDir.resolve("idx_random_dml.db");
        TreeMap<Integer, AccountRow> oracle = new TreeMap<>();
        Random rng = new Random(123456L);

        try (Database db = Database.open(dbFile, 64)) {
            db.execute("""
                    CREATE TABLE members (
                        id INT PRIMARY KEY,
                        code VARCHAR(32) NOT NULL,
                        score INT NOT NULL,
                        payload VARCHAR(200) NOT NULL
                    );
                    CREATE UNIQUE INDEX idx_members_code ON members (code);
                    CREATE INDEX idx_members_score ON members (score);
                    """);

            for (int step = 1; step <= 350; step++) {
                int action = rng.nextInt(100);
                int id = rng.nextInt(120);

                if (action < 45 || oracle.isEmpty()) {
                    if (!oracle.containsKey(id)) {
                        String code = "code_" + id + "_" + step;
                        int score = rng.nextInt(30);
                        String payload = "p_" + "A".repeat(10 + rng.nextInt(40));
                        db.execute(String.format(
                                "INSERT INTO members VALUES (%d, '%s', %d, '%s');",
                                id, code, score, payload));
                        oracle.put(id, new AccountRow(id, code, score, payload));
                    } else {
                        // Verify duplicate PK is rejected
                        assertThrows(
                                DuplicateKeyException.class,
                                () -> db.execute(String.format(
                                        "INSERT INTO members VALUES (%d, 'dup_%d', 1, 'x');", id, id)));
                    }
                } else if (action < 75) {
                    // Update an existing row (frequently expanding payload to trigger cross-page relocation)
                    List<Integer> keys = new ArrayList<>(oracle.keySet());
                    int targetId = keys.get(rng.nextInt(keys.size()));
                    String newCode = "code_" + targetId + "_u" + step;
                    int newScore = rng.nextInt(30);
                    String newPayload = "U".repeat(20 + rng.nextInt(170));
                    db.execute(String.format(
                            "UPDATE members SET code = '%s', score = %d, payload = '%s' WHERE id = %d;",
                            newCode, newScore, newPayload, targetId));
                    oracle.put(targetId, new AccountRow(targetId, newCode, newScore, newPayload));
                } else {
                    // Delete an existing row
                    List<Integer> keys = new ArrayList<>(oracle.keySet());
                    int targetId = keys.get(rng.nextInt(keys.size()));
                    db.execute("DELETE FROM members WHERE id = " + targetId + ";");
                    oracle.remove(targetId);
                }

                if (step % 70 == 0) {
                    verifyIndexesAndOracle(db, oracle);
                }
            }

            verifyIndexesAndOracle(db, oracle);
        }

        // Reopen Database and verify all indexes and data persisted cleanly
        try (Database reopened = Database.open(dbFile, 64)) {
            TableMetadata members = reopened.getCatalog().getTable("members");
            assertNotNull(members);
            assertEquals(3, members.getIndexes().size());

            verifyIndexesAndOracle(reopened, oracle);

            // Direct IndexScanExecutor range verification after reopen
            IndexMetadata scoreIdx = members.getIndex("idx_members_score");
            assertNotNull(scoreIdx);
            try (IndexScanExecutor scan = IndexScanExecutor.forRange(
                    members, scoreIdx, Value.ofInt(10), true, Value.ofInt(20), false)) {
                scan.init();
                int count = 0;
                while (scan.next() != null) {
                    count++;
                }
                long expectedCount = oracle.values().stream()
                        .filter(r -> r.score >= 10 && r.score < 20)
                        .count();
                assertEquals(expectedCount, count);
            }
        }
    }

    private record AccountRow(int id, String code, int score, String payload) {}

    private void verifyIndexesAndOracle(Database db, TreeMap<Integer, AccountRow> oracle) {
        TableMetadata table = db.getCatalog().getTable("members");
        for (IndexMetadata idx : table.getIndexes()) {
            BPlusTree tree = table.getIndexTree(idx);
            tree.verifyInvariants();
        }

        ResultSet allRows = db.execute("SELECT id, code, score, payload FROM members ORDER BY id ASC;");
        assertEquals(oracle.size(), allRows.rows().size());

        int i = 0;
        for (Map.Entry<Integer, AccountRow> entry : oracle.entrySet()) {
            AccountRow expected = entry.getValue();
            Tuple row = allRows.rows().get(i++);
            assertEquals(Value.ofInt(expected.id), row.getValue(0));
            assertEquals(Value.ofVarchar(expected.code), row.getValue(1));
            assertEquals(Value.ofInt(expected.score), row.getValue(2));
            assertEquals(Value.ofVarchar(expected.payload), row.getValue(3));

            // Verify lookup by unique code index
            ResultSet byCode = db.execute(
                    "SELECT id, payload FROM members WHERE code = '" + expected.code + "';");
            assertEquals(1, byCode.rows().size());
            assertEquals(Value.ofInt(expected.id), byCode.rows().get(0).getValue(0));
            assertEquals(Value.ofVarchar(expected.payload), byCode.rows().get(0).getValue(1));
        }
    }
}
