package io.nexusdb.core.parser;

/**
 * Thrown when {@link Parser} encounters a syntax error, carrying the 1-based line and column
 * position of the offending token.
 */
public final class ParseException extends RuntimeException {
    private final int line;
    private final int column;

    public ParseException(String message, int line, int column) {
        super(formatMessage(message, line, column));
        this.line = line;
        this.column = column;
    }

    public ParseException(String message, Token token) {
        this(message, token.line(), token.column());
    }

    public ParseException(String message, int line, int column, Throwable cause) {
        super(formatMessage(message, line, column), cause);
        this.line = line;
        this.column = column;
    }

    private static String formatMessage(String message, int line, int column) {
        return message + " (at line " + line + ", column " + column + ")";
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }
}
