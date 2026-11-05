package io.nexusdb.core.parser;

/**
 * Thrown when {@link Lexer} encounters an invalid character, malformed numeric literal,
 * or unterminated string/block comment, with 1-based line and column coordinates.
 */
public final class LexerException extends RuntimeException {
    private final int line;
    private final int column;

    public LexerException(String message, int line, int column) {
        super(formatMessage(message, line, column));
        this.line = line;
        this.column = column;
    }

    public LexerException(String message, int line, int column, Throwable cause) {
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
