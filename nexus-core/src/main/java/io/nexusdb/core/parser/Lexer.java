package io.nexusdb.core.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Single-pass lexical scanner for the NexusDB SQL dialect.
 *
 * <p>Position Tracking Invariant:
 * {@code line} and {@code column} are 1-based cursor coordinates updated exclusively in
 * {@link #advance()}. CRLF ({@code \r\n}) sequences increment {@code line} once on the
 * {@code \n} character, ensuring consistent column and line numbers across Windows and Unix inputs.
 */
public final class Lexer {
    private final String input;
    private final int length;
    private int pos = 0;
    private int line = 1;
    private int column = 1;

    public Lexer(String input) {
        this.input = Objects.requireNonNull(input, "input must not be null");
        this.length = input.length();
    }

    public static List<Token> tokenize(String sql) {
        return new Lexer(sql).scanTokens();
    }

    /**
     * Scans the entire SQL input and returns an immutable list of {@link Token}s ending with {@link TokenType#EOF}.
     */
    public List<Token> scanTokens() {
        List<Token> tokens = new ArrayList<>();
        while (true) {
            skipWhitespaceAndComments();
            int startLine = line;
            int startColumn = column;
            if (isAtEnd()) {
                tokens.add(new Token(TokenType.EOF, "", null, startLine, startColumn));
                break;
            }
            tokens.add(scanNextToken(startLine, startColumn));
        }
        return List.copyOf(tokens);
    }

    private Token scanNextToken(int startLine, int startColumn) {
        int startPos = pos;
        char c = advance();

        return switch (c) {
            case '(' -> new Token(TokenType.LPAREN, "(", null, startLine, startColumn);
            case ')' -> new Token(TokenType.RPAREN, ")", null, startLine, startColumn);
            case ',' -> new Token(TokenType.COMMA, ",", null, startLine, startColumn);
            case ';' -> new Token(TokenType.SEMICOLON, ";", null, startLine, startColumn);
            case '.' -> new Token(TokenType.DOT, ".", null, startLine, startColumn);
            case '+' -> new Token(TokenType.PLUS, "+", null, startLine, startColumn);
            case '-' -> new Token(TokenType.MINUS, "-", null, startLine, startColumn);
            case '*' -> new Token(TokenType.STAR, "*", null, startLine, startColumn);
            case '/' -> new Token(TokenType.SLASH, "/", null, startLine, startColumn);
            case '=' -> new Token(TokenType.EQ, "=", null, startLine, startColumn);
            case '!' -> {
                if (match('=')) {
                    yield new Token(TokenType.NEQ, "!=", null, startLine, startColumn);
                }
                throw new LexerException("Unexpected character '!' (expected '!=')", startLine, startColumn);
            }
            case '<' -> {
                if (match('=')) {
                    yield new Token(TokenType.LTE, "<=", null, startLine, startColumn);
                }
                if (match('>')) {
                    yield new Token(TokenType.NEQ, "<>", null, startLine, startColumn);
                }
                yield new Token(TokenType.LT, "<", null, startLine, startColumn);
            }
            case '>' -> {
                if (match('=')) {
                    yield new Token(TokenType.GTE, ">=", null, startLine, startColumn);
                }
                yield new Token(TokenType.GT, ">", null, startLine, startColumn);
            }
            case '\'' -> scanStringLiteral(startPos, startLine, startColumn);
            default -> {
                if (isAsciiDigit(c)) {
                    yield scanNumericLiteral(startPos, startLine, startColumn);
                }
                if (isIdentifierStart(c)) {
                    yield scanIdentifierOrKeyword(startPos, startLine, startColumn);
                }
                throw new LexerException("Unexpected character '" + c + "'", startLine, startColumn);
            }
        };
    }

    private void skipWhitespaceAndComments() {
        while (!isAtEnd()) {
            char c = peek();
            if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
                advance();
            } else if (c == '-' && peekNext() == '-') {
                // Single-line comment: -- until newline or EOF
                advance();
                advance();
                while (!isAtEnd() && peek() != '\n') {
                    advance();
                }
            } else if (c == '/' && peekNext() == '*') {
                // Multi-line block comment: /* ... */
                int commentLine = line;
                int commentCol = column;
                advance();
                advance();
                boolean closed = false;
                while (!isAtEnd()) {
                    if (peek() == '*' && peekNext() == '/') {
                        advance();
                        advance();
                        closed = true;
                        break;
                    }
                    advance();
                }
                if (!closed) {
                    throw new LexerException("Unterminated block comment", commentLine, commentCol);
                }
            } else {
                break;
            }
        }
    }

    private Token scanStringLiteral(int startPos, int startLine, int startColumn) {
        StringBuilder decoded = new StringBuilder();
        while (!isAtEnd()) {
            char c = advance();
            if (c == '\'') {
                // SQL standard doubled single quote ''
                if (!isAtEnd() && peek() == '\'') {
                    advance();
                    decoded.append('\'');
                } else {
                    String lexeme = input.substring(startPos, pos);
                    return new Token(TokenType.STRING_LITERAL, lexeme, decoded.toString(), startLine, startColumn);
                }
            } else if (c == '\\') {
                if (isAtEnd()) {
                    throw new LexerException("Unterminated string literal after escape", startLine, startColumn);
                }
                char escaped = advance();
                switch (escaped) {
                    case '\'' -> decoded.append('\'');
                    case '\\' -> decoded.append('\\');
                    case 'n' -> decoded.append('\n');
                    case 'r' -> decoded.append('\r');
                    case 't' -> decoded.append('\t');
                    default -> {
                        decoded.append('\\');
                        decoded.append(escaped);
                    }
                }
            } else {
                decoded.append(c);
            }
        }
        throw new LexerException("Unterminated string literal", startLine, startColumn);
    }

    private Token scanNumericLiteral(int startPos, int startLine, int startColumn) {
        boolean isFloat = false;
        while (!isAtEnd() && isAsciiDigit(peek())) {
            advance();
        }

        if (!isAtEnd() && peek() == '.' && isAsciiDigit(peekNext())) {
            isFloat = true;
            advance(); // consume '.'
            while (!isAtEnd() && isAsciiDigit(peek())) {
                advance();
            }
        }

        if (!isAtEnd() && (peek() == 'e' || peek() == 'E')) {
            isFloat = true;
            advance(); // consume 'e' or 'E'
            if (!isAtEnd() && (peek() == '+' || peek() == '-')) {
                advance();
            }
            if (isAtEnd() || !isAsciiDigit(peek())) {
                throw new LexerException(
                        "Invalid scientific notation in numeric literal '" + input.substring(startPos, pos) + "'",
                        startLine,
                        startColumn);
            }
            while (!isAtEnd() && isAsciiDigit(peek())) {
                advance();
            }
        }

        if (!isAtEnd() && isIdentifierStart(peek())) {
            int errLine = line;
            int errCol = column;
            throw new LexerException(
                    "Invalid trailing character '" + peek() + "' in numeric literal",
                    errLine,
                    errCol);
        }

        String lexeme = input.substring(startPos, pos);
        if (isFloat) {
            try {
                double val = Double.parseDouble(lexeme);
                if (Double.isInfinite(val) || Double.isNaN(val)) {
                    throw new LexerException("Floating-point literal out of range: " + lexeme, startLine, startColumn);
                }
                return new Token(TokenType.FLOAT_LITERAL, lexeme, val, startLine, startColumn);
            } catch (NumberFormatException e) {
                throw new LexerException("Invalid floating-point literal: " + lexeme, startLine, startColumn, e);
            }
        } else {
            try {
                long val = Long.parseLong(lexeme);
                return new Token(TokenType.INTEGER_LITERAL, lexeme, val, startLine, startColumn);
            } catch (NumberFormatException e) {
                throw new LexerException("Integer literal out of 64-bit BIGINT range: " + lexeme, startLine, startColumn, e);
            }
        }
    }

    private Token scanIdentifierOrKeyword(int startPos, int startLine, int startColumn) {
        while (!isAtEnd() && isIdentifierPart(peek())) {
            advance();
        }
        String lexeme = input.substring(startPos, pos);
        TokenType keywordType = TokenType.lookupKeyword(lexeme);
        if (keywordType != null) {
            Object literal = switch (keywordType) {
                case TRUE -> Boolean.TRUE;
                case FALSE -> Boolean.FALSE;
                default -> null;
            };
            return new Token(keywordType, lexeme, literal, startLine, startColumn);
        }
        return new Token(TokenType.IDENTIFIER, lexeme, lexeme, startLine, startColumn);
    }

    private boolean isAtEnd() {
        return pos >= length;
    }

    private char peek() {
        return isAtEnd() ? '\0' : input.charAt(pos);
    }

    private char peekNext() {
        return (pos + 1 >= length) ? '\0' : input.charAt(pos + 1);
    }

    private char advance() {
        char c = input.charAt(pos++);
        if (c == '\n') {
            line++;
            column = 1;
        } else if (c != '\r') {
            column++;
        }
        return c;
    }

    private boolean match(char expected) {
        if (isAtEnd() || input.charAt(pos) != expected) {
            return false;
        }
        advance();
        return true;
    }

    private static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isIdentifierStart(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }

    private static boolean isIdentifierPart(char c) {
        return isIdentifierStart(c) || isAsciiDigit(c);
    }
}
