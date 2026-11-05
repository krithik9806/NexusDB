package io.nexusdb.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class NexusCliIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void testScriptFileExecutionPersistenceAndInteractiveMetaCommands() throws IOException {
        Path dbDir = tempDir.resolve("cli-db");
        Path sqlFile = tempDir.resolve("setup.sql");

        Files.writeString(sqlFile, """
                CREATE TABLE customers (
                    id INT PRIMARY KEY,
                    name VARCHAR(40) NOT NULL,
                    tier VARCHAR(16),
                    credit DOUBLE NOT NULL
                );
                INSERT INTO customers VALUES
                    (1, 'Ada Lovelace', 'Platinum', 5000.0),
                    (2, 'Grace Hopper', 'Gold', 3500.0),
                    (3, 'Alan Turing', NULL, 4200.0);
                UPDATE customers SET credit = credit + 250.0 WHERE id = 1;
                """, StandardCharsets.UTF_8);

        // 1. Run non-interactive SQL script via --db and --file
        ByteArrayOutputStream batchOut = new ByteArrayOutputStream();
        ByteArrayOutputStream batchErr = new ByteArrayOutputStream();
        NexusCli batchCli = new NexusCli();
        int exitCode = batchCli.run(
                new String[]{"--db", dbDir.toString(), "--file", sqlFile.toString()},
                new ByteArrayInputStream(new byte[0]),
                new PrintStream(batchOut, true, StandardCharsets.UTF_8),
                new PrintStream(batchErr, true, StandardCharsets.UTF_8));

        assertEquals(0, exitCode, () -> "Stderr: " + batchErr.toString(StandardCharsets.UTF_8));
        assertEquals("", batchErr.toString(StandardCharsets.UTF_8));
        String batchOutput = batchOut.toString(StandardCharsets.UTF_8);
        assertTrue(batchOutput.contains("CREATE TABLE customers"));
        assertTrue(batchOutput.contains("INSERT 3"));
        assertTrue(batchOutput.contains("UPDATE 1"));

        // 2. Reopen against the same DB directory in interactive REPL mode and verify persistence,
        //    multi-line SQL statements, .tables, .schema, .history, and .quit
        String replInput = """
                .tables
                .schema customers
                SELECT id, name, tier, credit
                FROM customers
                WHERE credit >= 4000.0
                ORDER BY credit DESC;
                .quit
                """;
        ByteArrayOutputStream replOut = new ByteArrayOutputStream();
        ByteArrayOutputStream replErr = new ByteArrayOutputStream();
        NexusCli replCli = new NexusCli();
        int replExit = replCli.run(
                new String[]{"--db", dbDir.toString()},
                new ByteArrayInputStream(replInput.getBytes(StandardCharsets.UTF_8)),
                new PrintStream(replOut, true, StandardCharsets.UTF_8),
                new PrintStream(replErr, true, StandardCharsets.UTF_8));

        assertEquals(0, replExit);
        assertEquals("", replErr.toString(StandardCharsets.UTF_8));

        String output = replOut.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("customers"));
        assertTrue(output.contains(
                "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(40) NOT NULL, tier VARCHAR(16), credit DOUBLE NOT NULL);"));
        assertTrue(output.contains("| 1  | Ada Lovelace | Platinum | 5250.0 |"));
        assertTrue(output.contains("| 3  | Alan Turing  | NULL     | 4200.0 |"));
        assertTrue(output.contains("(2 rows)"));
        assertEquals(4, replCli.getHistory().size());
    }

    @Test
    void testUserFriendlyErrorsWithoutStackTracesInRepl() {
        Path dbDir = tempDir.resolve("err-db");
        String replInput = """
                SELECT FROM broken_syntax;
                SELECT * FROM missing_table;
                .unknown_cmd
                .exit
                """;
        ByteArrayOutputStream replOut = new ByteArrayOutputStream();
        ByteArrayOutputStream replErr = new ByteArrayOutputStream();
        NexusCli cli = new NexusCli();
        int exit = cli.run(
                new String[]{"--db", dbDir.toString()},
                new ByteArrayInputStream(replInput.getBytes(StandardCharsets.UTF_8)),
                new PrintStream(replOut, true, StandardCharsets.UTF_8),
                new PrintStream(replErr, true, StandardCharsets.UTF_8));

        assertEquals(0, exit);
        String errors = replErr.toString(StandardCharsets.UTF_8);
        assertTrue(errors.contains("Syntax error:"));
        assertTrue(errors.contains("Semantic error: Unknown table: missing_table"));
        assertTrue(errors.contains("Error: Unknown meta-command '.unknown_cmd'"));
        assertFalse(errors.contains("at io.nexusdb."), "Should not print raw Java stack traces");
    }
}
