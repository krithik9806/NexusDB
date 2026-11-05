package io.nexusdb.core.parser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LexerTest {

    @Test
    void testAllKeywordsCaseInsensitive() {
        String sql = """
                select FROM Where insert INTO values update SET delete
                CREATE drop DATABASE table INDEX unique ON primary KEY
                not NULL is AND or AS order BY asc DESC limit
                EXPLAIN analyze BEGIN commit ROLLBACK group HAVING
                count SUM avg MIN max JOIN inner LEFT outer
                int INTEGER bigint DOUBLE float BOOLEAN bool VARCHAR
                true FALSE
                """;
        List<Token> tokens = Lexer.tokenize(sql);
        TokenType[] expected = {
                TokenType.SELECT, TokenType.FROM, TokenType.WHERE, TokenType.INSERT, TokenType.INTO,
                TokenType.VALUES, TokenType.UPDATE, TokenType.SET, TokenType.DELETE,
                TokenType.CREATE, TokenType.DROP, TokenType.DATABASE, TokenType.TABLE, TokenType.INDEX,
                TokenType.UNIQUE, TokenType.ON, TokenType.PRIMARY, TokenType.KEY,
                TokenType.NOT, TokenType.NULL, TokenType.IS, TokenType.AND, TokenType.OR,
                TokenType.AS, TokenType.ORDER, TokenType.BY, TokenType.ASC, TokenType.DESC, TokenType.LIMIT,
                TokenType.EXPLAIN, TokenType.ANALYZE, TokenType.BEGIN, TokenType.COMMIT, TokenType.ROLLBACK,
                TokenType.GROUP, TokenType.HAVING,
                TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MIN, TokenType.MAX,
                TokenType.JOIN, TokenType.INNER, TokenType.LEFT, TokenType.OUTER,
                TokenType.INT, TokenType.INTEGER, TokenType.BIGINT, TokenType.DOUBLE, TokenType.FLOAT,
                TokenType.BOOLEAN, TokenType.BOOL, TokenType.VARCHAR,
                TokenType.TRUE, TokenType.FALSE,
                TokenType.EOF
        };
        assertEquals(expected.length, tokens.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], tokens.get(i).type(), "Mismatch at token index " + i);
            if (expected[i] != TokenType.EOF) {
                assertTrue(expected[i].isKeyword());
            }
        }
        assertEquals(Boolean.TRUE, tokens.get(tokens.size() - 3).literal());
        assertEquals(Boolean.FALSE, tokens.get(tokens.size() - 2).literal());
    }

    @Test
    void testOperatorsAndPunctuation() {
        String sql = "= != <> < <= > >= + - * / ( ) , ; .";
        List<Token> tokens = Lexer.tokenize(sql);
        TokenType[] expected = {
                TokenType.EQ, TokenType.NEQ, TokenType.NEQ,
                TokenType.LT, TokenType.LTE, TokenType.GT, TokenType.GTE,
                TokenType.PLUS, TokenType.MINUS, TokenType.STAR, TokenType.SLASH,
                TokenType.LPAREN, TokenType.RPAREN, TokenType.COMMA, TokenType.SEMICOLON, TokenType.DOT,
                TokenType.EOF
        };
        assertEquals(expected.length, tokens.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], tokens.get(i).type());
        }
        assertEquals("!=", tokens.get(1).lexeme());
        assertEquals("<>", tokens.get(2).lexeme());
    }

    @Test
    void testIdentifiersAndNumericLiterals() {
        String sql = "users _col_1 u.id 0 42 9223372036854775807 3.14159 0.5 2.5e3 1E-4";
        List<Token> tokens = Lexer.tokenize(sql);

        assertEquals(TokenType.IDENTIFIER, tokens.get(0).type());
        assertEquals("users", tokens.get(0).lexeme());

        assertEquals(TokenType.IDENTIFIER, tokens.get(1).type());
        assertEquals("_col_1", tokens.get(1).lexeme());

        assertEquals(TokenType.IDENTIFIER, tokens.get(2).type());
        assertEquals(TokenType.DOT, tokens.get(3).type());
        assertEquals(TokenType.IDENTIFIER, tokens.get(4).type());

        assertEquals(TokenType.INTEGER_LITERAL, tokens.get(5).type());
        assertEquals(0L, tokens.get(5).literal());

        assertEquals(TokenType.INTEGER_LITERAL, tokens.get(6).type());
        assertEquals(42L, tokens.get(6).literal());

        assertEquals(TokenType.INTEGER_LITERAL, tokens.get(7).type());
        assertEquals(Long.MAX_VALUE, tokens.get(7).literal());

        assertEquals(TokenType.FLOAT_LITERAL, tokens.get(8).type());
        assertEquals(3.14159, (Double) tokens.get(8).literal(), 1e-9);

        assertEquals(TokenType.FLOAT_LITERAL, tokens.get(9).type());
        assertEquals(0.5, (Double) tokens.get(9).literal(), 1e-9);

        assertEquals(TokenType.FLOAT_LITERAL, tokens.get(10).type());
        assertEquals(2500.0, (Double) tokens.get(10).literal(), 1e-9);

        assertEquals(TokenType.FLOAT_LITERAL, tokens.get(11).type());
        assertEquals(1e-4, (Double) tokens.get(11).literal(), 1e-9);

        assertEquals(TokenType.EOF, tokens.get(12).type());
    }

    @Test
    void testStringLiteralsWithEscapedQuotes() {
        String sql = "'hello world' 'it''s a test' 'escaped \\'quote\\' and \\\\slash' ''";
        List<Token> tokens = Lexer.tokenize(sql);
        assertEquals(5, tokens.size());

        assertEquals(TokenType.STRING_LITERAL, tokens.get(0).type());
        assertEquals("hello world", tokens.get(0).literal());

        assertEquals(TokenType.STRING_LITERAL, tokens.get(1).type());
        assertEquals("it's a test", tokens.get(1).literal());

        assertEquals(TokenType.STRING_LITERAL, tokens.get(2).type());
        assertEquals("escaped 'quote' and \\slash", tokens.get(2).literal());

        assertEquals(TokenType.STRING_LITERAL, tokens.get(3).type());
        assertEquals("", tokens.get(3).literal());
    }

    @Test
    void testSingleLineAndBlockComments() {
        String sql = """
                -- Header comment
                SELECT /* inline comment */ id, -- end of line comment
                /* multi-line
                   comment with * and / inside */
                name FROM users;
                """;
        List<Token> tokens = Lexer.tokenize(sql);
        TokenType[] expected = {
                TokenType.SELECT, TokenType.IDENTIFIER, TokenType.COMMA,
                TokenType.IDENTIFIER, TokenType.FROM, TokenType.IDENTIFIER,
                TokenType.SEMICOLON, TokenType.EOF
        };
        assertEquals(expected.length, tokens.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], tokens.get(i).type());
        }
        // Verify line and column tracking after multi-line comment
        Token selectTok = tokens.get(0);
        assertEquals(2, selectTok.line());
        assertEquals(1, selectTok.column());

        Token idTok = tokens.get(1);
        assertEquals(2, idTok.line());
        assertEquals(29, idTok.column());

        Token nameTok = tokens.get(3);
        assertEquals(5, nameTok.line());
        assertEquals(1, nameTok.column());
    }

    @Test
    void testAccurateLineAndColumnPositionsWithCrlfAndLf() {
        String sql = "SELECT a\r\n  FROM t\n    WHERE x >= 10;";
        List<Token> tokens = Lexer.tokenize(sql);

        assertEquals(1, tokens.get(0).line());
        assertEquals(1, tokens.get(0).column()); // SELECT

        assertEquals(1, tokens.get(1).line());
        assertEquals(8, tokens.get(1).column()); // a

        assertEquals(2, tokens.get(2).line());
        assertEquals(3, tokens.get(2).column()); // FROM

        assertEquals(2, tokens.get(3).line());
        assertEquals(8, tokens.get(3).column()); // t

        assertEquals(3, tokens.get(4).line());
        assertEquals(5, tokens.get(4).column()); // WHERE

        assertEquals(3, tokens.get(5).line());
        assertEquals(11, tokens.get(5).column()); // x

        assertEquals(3, tokens.get(6).line());
        assertEquals(13, tokens.get(6).column()); // >=

        assertEquals(3, tokens.get(7).line());
        assertEquals(16, tokens.get(7).column()); // 10
    }

    @Test
    void testUnterminatedStringThrowsLexerExceptionWithPosition() {
        String sql = "SELECT 'valid',\n  'unterminated string";
        LexerException ex = assertThrows(LexerException.class, () -> Lexer.tokenize(sql));
        assertEquals(2, ex.getLine());
        assertEquals(3, ex.getColumn());
        assertTrue(ex.getMessage().contains("line 2, column 3"));
    }

    @Test
    void testUnterminatedBlockCommentThrowsLexerExceptionWithPosition() {
        String sql = "SELECT 1;\n   /* never closed\nstill in comment";
        LexerException ex = assertThrows(LexerException.class, () -> Lexer.tokenize(sql));
        assertEquals(2, ex.getLine());
        assertEquals(4, ex.getColumn());
    }

    @Test
    void testInvalidCharacterAndStandaloneBangThrowLexerException() {
        LexerException ex1 = assertThrows(LexerException.class, () -> Lexer.tokenize("SELECT @x FROM t"));
        assertEquals(1, ex1.getLine());
        assertEquals(8, ex1.getColumn());

        LexerException ex2 = assertThrows(LexerException.class, () -> Lexer.tokenize("SELECT a ! b"));
        assertEquals(1, ex2.getLine());
        assertEquals(10, ex2.getColumn());
    }

    @Test
    void testMalformedNumberThrowsLexerException() {
        LexerException ex1 = assertThrows(LexerException.class, () -> Lexer.tokenize("SELECT 123abc"));
        assertEquals(1, ex1.getLine());
        assertEquals(11, ex1.getColumn());

        LexerException ex2 = assertThrows(LexerException.class, () -> Lexer.tokenize("SELECT 1.5e+"));
        assertEquals(1, ex2.getLine());
        assertEquals(8, ex2.getColumn());
    }
}
