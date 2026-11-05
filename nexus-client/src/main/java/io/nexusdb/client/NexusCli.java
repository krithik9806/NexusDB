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
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.proto.SqlRow;

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
 * Supports both local embedded databases and remote connections over gRPC.
 */
public final class NexusCli {
    private static final String PROMPT = "nexusdb> ";
    private static final String CONTINUATION_PROMPT = "      -> ";

    private final List<String> history = new ArrayList<>();

    public List<String> getHistory() {
        return Collections.unmodifiableList(history);
    }

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
        String host = null;
        int port = 9000;
        boolean tls = false;

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
                case "--host", "-h" -> {
                    if (i + 1 >= args.length) {
                        err.println("Error: Missing host argument for " + arg);
                        return 1;
                    }
                    host = args[++i];
                }
                case "--port", "-p" -> {
                    if (i + 1 >= args.length) {
                        err.println("Error: Missing port argument for " + arg);
                        return 1;
                    }
                    port = Integer.parseInt(args[++i]);
                }
                case "--tls" -> {
                    tls = true;
                }
                default -> {
                    err.println("Error: Unknown option '" + arg + "'. Usage: NexusCli [--db <path>] [--host <host>] [--port <port>] [--tls] [--file <path>]");
                    return 1;
                }
            }
        }

        if (host != null) {
            try (NexusClient client = new NexusClient(host, port, tls)) {
                if (scriptFile != null) {
                    if (!Files.exists(scriptFile)) {
                        err.println("Error: SQL script file does not exist: " + scriptFile);
                        return 1;
                    }
                    String scriptContent = Files.readString(scriptFile, StandardCharsets.UTF_8);
                    return executeRemoteScript(client, scriptContent, out, err);
                }
                runRemoteRepl(client, in, out, err);
                return 0;
            } catch (Exception e) {
                err.println("Remote connection error: " + e.getMessage());
                return 1;
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

    private int executeRemoteScript(NexusClient client, String scriptContent, PrintStream out, PrintStream err) {
        String[] stmts = scriptContent.split(";");
        for (String raw : stmts) {
            String sql = raw.trim();
            if (sql.isEmpty()) {
                continue;
            }
            SqlResponse resp = client.execute(sql);
            if (!resp.getSuccess()) {
                err.println("Error: " + resp.getErrorMessage());
                return 1;
            }
            printSqlResponse(resp, out);
        }
        return 0;
    }

    private void runRemoteRepl(NexusClient client, InputStream in, PrintStream out, PrintStream err) throws IOException {
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
                if (trimmed.equalsIgnoreCase(".quit") || trimmed.equalsIgnoreCase(".exit")) {
                    break;
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
                try {
                    SqlResponse resp = client.execute(fullSql);
                    if (!resp.getSuccess()) {
                        err.println("Error: " + resp.getErrorMessage());
                    } else {
                        printSqlResponse(resp, out);
                    }
                } catch (Exception e) {
                    err.println("gRPC error: " + e.getMessage());
                }
            }
        }
    }

    private static void printSqlResponse(SqlResponse resp, PrintStream out) {
        if (resp.getColumnsCount() == 0) {
            out.println("Query OK, " + resp.getRowsAffected() + " rows affected (" + String.format(Locale.ROOT, "%.2f", resp.getExecutionTimeMs()) + " ms)");
            return;
        }

        List<String> cols = resp.getColumnsList();
        List<List<String>> rows = new ArrayList<>();
        for (SqlRow r : resp.getRowsList()) {
            rows.add(r.getValuesList());
        }

        int[] widths = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            widths[i] = cols.get(i).length();
        }
        for (List<String> r : rows) {
            for (int i = 0; i < cols.size(); i++) {
                String v = i < r.size() ? r.get(i) : "NULL";
                widths[i] = Math.max(widths[i], v.length());
            }
        }

        StringBuilder sep = new StringBuilder("+");
        for (int w : widths) {
            sep.append("-".repeat(w + 2)).append("+");
        }
        out.println(sep);

        StringBuilder header = new StringBuilder("|");
        for (int i = 0; i < cols.size(); i++) {
            header.append(" ").append(padRight(cols.get(i), widths[i])).append(" |");
        }
        out.println(header);
        out.println(sep);

        for (List<String> r : rows) {
            StringBuilder row = new StringBuilder("|");
            for (int i = 0; i < cols.size(); i++) {
                String v = i < r.size() ? r.get(i) : "NULL";
                row.append(" ").append(padRight(v, widths[i])).append(" |");
            }
            out.println(row);
        }
        out.println(sep);
        out.println(rows.size() + " row" + (rows.size() == 1 ? "" : "s") + " in set (" + String.format(Locale.ROOT, "%.2f", resp.getExecutionTimeMs()) + " ms)");
    }

    private static String padRight(String s, int width) {
        if (s.length() >= width) {
            return s;
        }
        return s + " ".repeat(width - s.length());
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

    private static boolean hasTerminatingSemicolon(CharSequence buffer) {
        boolean inSingleQuotes = false;
        boolean inBlockComment = false;
        boolean inLineComment = false;
        char lastSignificantChar = '\0';

        int len = buffer.length();
        for (int i = 0; i < len; i++) {
            char c = buffer.charAt(i);
            char next = (i + 1 < len) ? buffer.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n' || c == '\r') {
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
            if (inSingleQuotes) {
                if (c == '\'') {
                    if (next == '\'') {
                        i++; // escaped quote
                    } else {
                        inSingleQuotes = false;
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
                inSingleQuotes = true;
                continue;
            }

            if (!Character.isWhitespace(c)) {
                lastSignificantChar = c;
            }
        }

        return !inSingleQuotes && !inBlockComment && lastSignificantChar == ';';
    }
}
