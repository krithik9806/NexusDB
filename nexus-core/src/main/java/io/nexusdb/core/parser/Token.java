package io.nexusdb.core.parser;

import java.util.Objects;

/**
 * Immutable lexical token produced by {@link Lexer}, carrying its {@link TokenType},
 * source {@code lexeme}, optional decoded {@code literal}, and 1-based source coordinates.
 */
public record Token(
        TokenType type,
        String lexeme,
        Object literal,
        int line,
        int column) {

    public Token {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(lexeme, "lexeme must not be null");
        if (line < 1) {
            throw new IllegalArgumentException("line must be >= 1, got: " + line);
        }
        if (column < 1) {
            throw new IllegalArgumentException("column must be >= 1, got: " + column);
        }
    }

    public Token(TokenType type, String lexeme, int line, int column) {
        this(type, lexeme, null, line, column);
    }

    public String stringValue() {
        return literal instanceof String s ? s : lexeme;
    }

    @Override
    public String toString() {
        if (literal != null) {
            return type + "(" + lexeme + ", literal=" + literal + ")@" + line + ":" + column;
        }
        return type + "(" + lexeme + ")@" + line + ":" + column;
    }
}
