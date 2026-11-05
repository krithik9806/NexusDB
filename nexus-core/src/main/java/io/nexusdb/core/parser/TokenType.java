package io.nexusdb.core.parser;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Lexical token types for the NexusDB SQL dialect (Phases 2–4b).
 */
public enum TokenType {
    // Keywords (DDL, DML, DQL, Transactions, Aggregates, Joins, Types)
    SELECT(true),
    FROM(true),
    WHERE(true),
    INSERT(true),
    INTO(true),
    VALUES(true),
    UPDATE(true),
    SET(true),
    DELETE(true),
    CREATE(true),
    DROP(true),
    DATABASE(true),
    TABLE(true),
    INDEX(true),
    UNIQUE(true),
    ON(true),
    PRIMARY(true),
    KEY(true),
    NOT(true),
    NULL(true),
    IS(true),
    AND(true),
    OR(true),
    AS(true),
    ORDER(true),
    BY(true),
    ASC(true),
    DESC(true),
    LIMIT(true),
    EXPLAIN(true),
    ANALYZE(true),
    BEGIN(true),
    COMMIT(true),
    ROLLBACK(true),
    GROUP(true),
    HAVING(true),
    COUNT(true),
    SUM(true),
    AVG(true),
    MIN(true),
    MAX(true),
    JOIN(true),
    INNER(true),
    LEFT(true),
    OUTER(true),
    INT(true),
    INTEGER(true),
    BIGINT(true),
    DOUBLE(true),
    FLOAT(true),
    BOOLEAN(true),
    BOOL(true),
    VARCHAR(true),
    TRUE(true),
    FALSE(true),

    // Identifiers and Literals
    IDENTIFIER(false),
    INTEGER_LITERAL(false),
    FLOAT_LITERAL(false),
    STRING_LITERAL(false),

    // Operators
    EQ(false),
    NEQ(false),
    LT(false),
    LTE(false),
    GT(false),
    GTE(false),
    PLUS(false),
    MINUS(false),
    STAR(false),
    SLASH(false),

    // Punctuation
    LPAREN(false),
    RPAREN(false),
    COMMA(false),
    SEMICOLON(false),
    DOT(false),

    // End of input
    EOF(false);

    private static final Map<String, TokenType> KEYWORDS;

    static {
        Map<String, TokenType> map = new HashMap<>();
        for (TokenType type : values()) {
            if (type.keyword) {
                map.put(type.name(), type);
            }
        }
        KEYWORDS = Collections.unmodifiableMap(map);
    }

    private final boolean keyword;

    TokenType(boolean keyword) {
        this.keyword = keyword;
    }

    public boolean isKeyword() {
        return keyword;
    }

    /**
     * Resolves an identifier string to a keyword {@link TokenType} (case-insensitive),
     * or returns {@code null} if it is not a reserved keyword.
     */
    public static TokenType lookupKeyword(String text) {
        if (text == null) {
            return null;
        }
        return KEYWORDS.get(text.toUpperCase(Locale.ROOT));
    }
}
