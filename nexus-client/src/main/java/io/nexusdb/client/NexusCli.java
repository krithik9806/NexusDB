package io.nexusdb.client;

import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.parser.LexerException;
import io.nexusdb.core.parser.ParseException;
import io.nexusdb.core.planner.BindException;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.storage.StorageException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * NexusDB interactive SQL REPL and non-interactive script runner.
 */
public final class NexusCli {
    private static final String PROMPT = "nexusdb> ";
    private static final String CONTINUATION_PROMPT = "      -> ";

    private final List<String> history = new ArrayList<>();

    public static void main(String[] args) {
        int code = new NexusCli().run(args, System.in, System.out, System.err);
        if (code != 0) {
            System.exit(code);
        }
    }

    public int run(String[] args, InputStream in, PrintStream out, PrintStream err) {
        Objects.requireNonNull(args, "args must not be null");
        Objects.requireNonNull(in, "in must not be null");
        Objects.requireNonNull(out, "out must not be null");
        Objects.requireNonNull(err, "err must not be null");

        Path dbPath = Path.of("nexusdb-data");
        Path scriptFile = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--db", "-d" -> {
                    if (i + 1 >= args.length) {
                        err.println("Error: Missing path argument for " + arg);
                        return 1;
                    }
                    dbPath = Path.of(args[++i]);
                }
                case "--file", "-f" -> {
                    if (i + 1 >= args.length) {
                        err.println("Error: Missing path argument for " + arg);
                        return 1;
                    }
                    scriptFile = Path.of(args[++i]);
                }
                default -> {
                    err.println("Error: Unknown option '" + arg + "'. Usage: NexusCli [--db <path>] [--file <path>]");
                    return 1;
                }
            }
        }

        try (Database db = Database.open(dbPath)) {
            if (scriptFile != null) {
                if (!Files.exists(scriptFile)) {
                    err.println("Error: SQL script file does not exist: " + scriptFile);
                    return 1;
                }
                String scriptContent = Files.readString(scriptFile, StandardCharsets.UTF_8);
                return executeBatch(db, scriptContent, out, err);
            }
            runInteractiveRepl(db, in, out, err);
            return 0;
        } catch (StorageException | IOException e) {
            err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private int executeBatch(Database db, String scriptContent, PrintStream out, PrintStream err) {
        try {
            List<ResultSet> results = db.executeScript(scriptContent);
            for (ResultSet rs : results) {
                printResultSet(rs, out);
            }
            return 0;
        } catch (ParseException | LexerException e) {
            err.println("Syntax error: " + e.getMessage());
            return 1;
        } catch (BindException e) {
            err.println("Semantic error: " + e.getMessage());
            return 1;
        } catch (StorageException e) {
            err.println("Storage error: " + e.getMessage());
            return 1;
        } catch (RuntimeException e) {
            err.println("Execution error: " + e.getMessage());
            return 1;
        }
    }

    private void runInteractiveRepl(Database db, InputStream in, PrintStream out, PrintStream err) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        StringBuilder statementBuffer = new StringBuilder();

        while (true) {
            out.print(statementBuffer.isEmpty() ? PROMPT : CONTINUATION_PROMPT);
            out.flush();

            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String trimmed = line.trim();
            if (statementBuffer.isEmpty()) {
                if (trimmed.isEmpty() || trimmed.startsWith("--")) {
                    continue;
                }
                if (trimmed.startsWith(".")) {
                    history.add(trimmed);
                    boolean shouldQuit = handleMetaCommand(db, trimmed, out, err);
                    if (shouldQuit) {
                        break;
                    }
                    continue;
                }
            }

            if (!statementBuffer.isEmpty()) {
                statementBuffer.append('\n');
            }
            statementBuffer.append(line);

            if (hasTerminatingSemicolon(statementBuffer)) {
                String fullSql = statementBuffer.toString().trim();
                statementBuffer.setLength(0);
                history.add(fullSql);
                executeInteractiveStatement(db, fullSql, out, err);
            }
        }
    }

    private void executeInteractiveStatement(Database db, String sql, PrintStream out, PrintStream err) {
        try {
            List<ResultSet> results = db.executeScript(sql);
            for (ResultSet rs : results) {
                printResultSet(rs, out);
            }
        } catch (ParseException | LexerException e) {
            err.println("Syntax error: " + e.getMessage());
        } catch (BindException e) {
            err.println("Semantic error: " + e.getMessage());
        } catch (StorageException e) {
            err.println("Storage error: " + e.getMessage());
        } catch (RuntimeException e) {
            err.println("Execution error: " + e.getMessage());
        }
    }

    private boolean handleMetaCommand(Database db, String line, PrintStream out, PrintStream err) {
        String[] parts = line.split("\\s+", 2);
        String cmd = parts[0].toLowerCase(Locale.ROOT);
        String arg = parts.length > 1 ? parts[1].trim() : "";

        switch (cmd) {
            case ".quit", ".exit" -> {
                return true;
            }
            case ".tables" -> {
                List<String> names = db.getCatalog().listTableNames();
                if (names.isEmpty()) {
                    out.println("(no tables)");
                } else {
                    for (String name : names) {
                        out.println(name);
                    }
                }
                return false;
            }
            case ".schema" -> {
                if (!arg.isEmpty()) {
                    TableMetadata meta = db.getCatalog().getTable(arg);
                    if (meta == null) {
                        err.println("Error: Unknown table '" + arg + "'");
                    } else {
                        out.println(formatCreateTableDdl(meta));
                    }
                } else {
                    List<TableMetadata> tables = db.getCatalog().listTables();
                    if (tables.isEmpty()) {
                        out.println("(no tables)");
                    } else {
                        for (TableMetadata meta : tables) {
                            out.println(formatCreateTableDdl(meta));
                        }
                    }
                }
                return false;
            }
            case ".history" -> {
                for (int i = 0; i < history.size(); i++) {
                    out.println((i + 1) + ": " + history.get(i));
                }
                return false;
            }
            default -> {
                err.println("Error: Unknown meta-command '" + cmd + "' (supported: .tables, .schema [table], .history, .quit, .exit)");
                return false;
            }
        }
    }

    public static String formatCreateTableDdl(TableMetadata meta) {
        String cols = meta.getSchema().getColumns().stream()
                .map(NexusCli::formatColumnDdl)
                .collect(Collectors.joining(", "));
        return "CREATE TABLE " + meta.getName() + " (" + cols + ");";
    }

    private static String formatColumnDdl(Column col) {
        StringBuilder sb = new StringBuilder(col.name()).append(' ');
        if (col.type() == DataType.VARCHAR) {
            sb.append("VARCHAR(").append(col.maxLength()).append(')');
        } else {
            sb.append(col.type().name());
        }
        if (col.primaryKey()) {
            sb.append(" PRIMARY KEY");
        } else if (!col.nullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private static void printResultSet(ResultSet rs, PrintStream out) {
        String formatted = rs.formatAsciiTable();
        if (!formatted.isEmpty()) {
            out.println(formatted);
        }
    }

    /**
     * Checks whether {@code buffer} contains an unquoted, uncommented semicolon followed only
     * by whitespace or comments.
     */
    static boolean hasTerminatingSemicolon(CharSequence buffer) {
        boolean inSingleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean seenTopLevelSemicolon = false;
        int len = buffer.length();

        for (int i = 0; i < len; i++) {
            char c = buffer.charAt(i);
            char next = (i + 1 < len) ? buffer.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }
            if (inSingleQuote) {
                if (c == '\\' && i + 1 < len) {
                    i++;
                } else if (c == '\'') {
                    if (next == '\'') {
                        i++;
                    } else {
                        inSingleQuote = false;
                    }
                }
                continue;
            }

            if (c == '-' && next == '-') {
                inLineComment = true;
                i++;
                continue;
            }
            if (c == '/' && next == '*') {
                inBlockComment = true;
                i++;
                continue;
            }
            if (c == '\'') {
                inSingleQuote = true;
                seenTopLevelSemicolon = false;
                continue;
            }
            if (c == ';') {
                seenTopLevelSemicolon = true;
            } else if (!Character.isWhitespace(c)) {
                seenTopLevelSemicolon = false;
            }
        }
        return seenTopLevelSemicolon;
    }

    public List<String> getHistory() {
        return Collections.unmodifiableList(history);
    }
}
