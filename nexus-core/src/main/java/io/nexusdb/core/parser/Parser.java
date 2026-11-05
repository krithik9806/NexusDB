package io.nexusdb.core.parser;

import io.nexusdb.core.ast.ColumnDef;
import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Recursive-descent SQL parser producing immutable AST nodes in {@code io.nexusdb.core.ast}.
 */
public final class Parser {
    private final List<Token> tokens;
    private int current = 0;

    public Parser(List<Token> tokens) {
        this.tokens = Objects.requireNonNull(tokens, "tokens must not be null");
        if (tokens.isEmpty() || tokens.get(tokens.size() - 1).type() != TokenType.EOF) {
            throw new IllegalArgumentException("Token stream must end with EOF token");
        }
    }

    public static Statement parse(String sql) {
        Objects.requireNonNull(sql, "sql must not be null");
        List<Token> tokens;
        try {
            tokens = Lexer.tokenize(sql);
        } catch (LexerException e) {
            throw new ParseException(e.getMessage(), e.getLine(), e.getColumn(), e);
        }
        return new Parser(tokens).parseStatement();
    }

    public Statement parseStatement() {
        if (isAtEnd()) {
            throw error(peek(), "Expected SQL statement but found end of input");
        }
        Statement stmt = switch (peek().type()) {
            case CREATE -> parseCreate();
            case DROP -> parseDrop();
            case INSERT -> parseInsert();
            default -> throw error(peek(), "Unexpected token '" + peek().lexeme() + "' at start of statement");
        };

        // Optional trailing semicolon
        match(TokenType.SEMICOLON);
        if (!isAtEnd()) {
            throw error(peek(), "Unexpected token '" + peek().lexeme() + "' after end of statement");
        }
        return stmt;
    }

    private Statement parseCreate() {
        consume(TokenType.CREATE, "Expected 'CREATE'");
        if (match(TokenType.DATABASE)) {
            Token dbName = consumeIdentifier("Expected database name after 'CREATE DATABASE'");
            return new Statement.CreateDatabaseStatement(dbName.lexeme());
        }
        if (match(TokenType.TABLE)) {
            return parseCreateTable();
        }
        throw error(peek(), "Expected 'DATABASE' or 'TABLE' after 'CREATE', found '" + peek().lexeme() + "'");
    }

    private Statement.CreateTableStatement parseCreateTable() {
        Token tableName = consumeIdentifier("Expected table name after 'CREATE TABLE'");
        consume(TokenType.LPAREN, "Expected '(' after table name '" + tableName.lexeme() + "'");
        if (check(TokenType.RPAREN)) {
            throw error(peek(), "CREATE TABLE '" + tableName.lexeme() + "' must define at least one column");
        }

        List<ColumnDef> columns = new ArrayList<>();
        do {
            columns.add(parseColumnDef());
        } while (match(TokenType.COMMA));

        consume(TokenType.RPAREN, "Expected ')' or ',' after column definition");
        return new Statement.CreateTableStatement(tableName.lexeme(), columns);
    }

    private ColumnDef parseColumnDef() {
        Token colName = consumeIdentifier("Expected column name in column definition");
        Token typeToken = peek();
        DataType dataType;
        int maxLength;

        if (match(TokenType.INT, TokenType.INTEGER)) {
            dataType = DataType.INT;
            maxLength = DataType.INT.getFixedSize();
        } else if (match(TokenType.BIGINT)) {
            dataType = DataType.BIGINT;
            maxLength = DataType.BIGINT.getFixedSize();
        } else if (match(TokenType.DOUBLE, TokenType.FLOAT)) {
            dataType = DataType.DOUBLE;
            maxLength = DataType.DOUBLE.getFixedSize();
        } else if (match(TokenType.BOOLEAN, TokenType.BOOL)) {
            dataType = DataType.BOOLEAN;
            maxLength = DataType.BOOLEAN.getFixedSize();
        } else if (match(TokenType.VARCHAR)) {
            dataType = DataType.VARCHAR;
            consume(TokenType.LPAREN, "Expected '(' after VARCHAR");
            Token lenTok = consume(TokenType.INTEGER_LITERAL, "Expected positive integer length inside VARCHAR(...)");
            long rawLen = (Long) lenTok.literal();
            if (rawLen <= 0 || rawLen > Short.MAX_VALUE) {
                throw error(lenTok, "VARCHAR length must be between 1 and " + Short.MAX_VALUE + ", got " + rawLen);
            }
            maxLength = (int) rawLen;
            consume(TokenType.RPAREN, "Expected ')' after VARCHAR length");
        } else {
            throw error(typeToken, "Expected column data type (INT, BIGINT, DOUBLE, BOOLEAN, VARCHAR) for column '"
                    + colName.lexeme() + "', found '" + typeToken.lexeme() + "'");
        }

        boolean primaryKey = false;
        boolean nullable = true;

        while (true) {
            if (match(TokenType.PRIMARY)) {
                consume(TokenType.KEY, "Expected 'KEY' after 'PRIMARY'");
                primaryKey = true;
                nullable = false;
            } else if (match(TokenType.NOT)) {
                consume(TokenType.NULL, "Expected 'NULL' after 'NOT'");
                nullable = false;
            } else {
                break;
            }
        }

        return new ColumnDef(colName.lexeme(), dataType, maxLength, nullable, primaryKey);
    }

    private Statement parseDrop() {
        consume(TokenType.DROP, "Expected 'DROP'");
        if (match(TokenType.TABLE)) {
            Token tableName = consumeIdentifier("Expected table name after 'DROP TABLE'");
            return new Statement.DropTableStatement(tableName.lexeme());
        }
        throw error(peek(), "Expected 'TABLE' after 'DROP', found '" + peek().lexeme() + "'");
    }

    private Statement.InsertStatement parseInsert() {
        consume(TokenType.INSERT, "Expected 'INSERT'");
        consume(TokenType.INTO, "Expected 'INTO' after 'INSERT'");
        Token tableName = consumeIdentifier("Expected table name after 'INSERT INTO'");

        List<String> columns = new ArrayList<>();
        if (match(TokenType.LPAREN)) {
            if (check(TokenType.RPAREN)) {
                throw error(peek(), "Column list in INSERT INTO cannot be empty");
            }
            do {
                Token col = consumeIdentifier("Expected column name in INSERT column list");
                columns.add(col.lexeme());
            } while (match(TokenType.COMMA));
            consume(TokenType.RPAREN, "Expected ')' after INSERT column list");
        }

        consume(TokenType.VALUES, "Expected 'VALUES' in INSERT statement");

        List<List<Expr>> rows = new ArrayList<>();
        do {
            consume(TokenType.LPAREN, "Expected '(' to start row values");
            if (check(TokenType.RPAREN)) {
                throw error(peek(), "VALUES row tuple cannot be empty");
            }
            List<Expr> row = new ArrayList<>();
            do {
                row.add(parseExpression());
            } while (match(TokenType.COMMA));
            consume(TokenType.RPAREN, "Expected ')' after row values");
            rows.add(row);
        } while (match(TokenType.COMMA));

        return new Statement.InsertStatement(tableName.lexeme(), columns, rows);
    }

    // Expression hierarchy (or -> and -> not -> predicate -> additive -> multiplicative -> unary -> primary)
    public Expr parseExpression() {
        return parseOrExpr();
    }

    private Expr parseOrExpr() {
        Expr expr = parseAndExpr();
        while (match(TokenType.OR)) {
            Expr right = parseAndExpr();
            expr = new Expr.BinaryExpr(expr, Expr.BinaryOp.OR, right);
        }
        return expr;
    }

    private Expr parseAndExpr() {
        Expr expr = parseNotExpr();
        while (match(TokenType.AND)) {
            Expr right = parseNotExpr();
            expr = new Expr.BinaryExpr(expr, Expr.BinaryOp.AND, right);
        }
        return expr;
    }

    private Expr parseNotExpr() {
        if (match(TokenType.NOT)) {
            return new Expr.UnaryExpr(Expr.UnaryOp.NOT, parseNotExpr());
        }
        return parsePredicateExpr();
    }

    private Expr parsePredicateExpr() {
        Expr left = parseAdditiveExpr();
        if (match(TokenType.IS)) {
            boolean negated = match(TokenType.NOT);
            consume(TokenType.NULL, "Expected 'NULL' after 'IS" + (negated ? " NOT" : "") + "'");
            return new Expr.IsNullExpr(left, negated);
        }
        if (match(TokenType.EQ)) {
            return new Expr.BinaryExpr(left, Expr.BinaryOp.EQ, parseAdditiveExpr());
        }
        if (match(TokenType.NEQ)) {
            return new Expr.BinaryExpr(left, Expr.BinaryOp.NEQ, parseAdditiveExpr());
        }
        if (match(TokenType.LT)) {
            return new Expr.BinaryExpr(left, Expr.BinaryOp.LT, parseAdditiveExpr());
        }
        if (match(TokenType.LTE)) {
            return new Expr.BinaryExpr(left, Expr.BinaryOp.LTE, parseAdditiveExpr());
        }
        if (match(TokenType.GT)) {
            return new Expr.BinaryExpr(left, Expr.BinaryOp.GT, parseAdditiveExpr());
        }
        if (match(TokenType.GTE)) {
            return new Expr.BinaryExpr(left, Expr.BinaryOp.GTE, parseAdditiveExpr());
        }
        return left;
    }

    private Expr parseAdditiveExpr() {
        Expr expr = parseMultiplicativeExpr();
        while (true) {
            if (match(TokenType.PLUS)) {
                expr = new Expr.BinaryExpr(expr, Expr.BinaryOp.ADD, parseMultiplicativeExpr());
            } else if (match(TokenType.MINUS)) {
                expr = new Expr.BinaryExpr(expr, Expr.BinaryOp.SUB, parseMultiplicativeExpr());
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr parseMultiplicativeExpr() {
        Expr expr = parseUnaryExpr();
        while (true) {
            if (match(TokenType.STAR)) {
                expr = new Expr.BinaryExpr(expr, Expr.BinaryOp.MUL, parseUnaryExpr());
            } else if (match(TokenType.SLASH)) {
                expr = new Expr.BinaryExpr(expr, Expr.BinaryOp.DIV, parseUnaryExpr());
            } else {
                break;
            }
        }
        return expr;
    }

    private Expr parseUnaryExpr() {
        if (match(TokenType.MINUS)) {
            return new Expr.UnaryExpr(Expr.UnaryOp.NEGATE, parseUnaryExpr());
        }
        return parsePrimaryExpr();
    }

    private Expr parsePrimaryExpr() {
        Token tok = peek();
        if (match(TokenType.INTEGER_LITERAL)) {
            long val = (Long) tok.literal();
            if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
                return new Expr.LiteralExpr(Value.ofInt((int) val));
            }
            return new Expr.LiteralExpr(Value.ofBigInt(val));
        }
        if (match(TokenType.FLOAT_LITERAL)) {
            return new Expr.LiteralExpr(Value.ofDouble((Double) tok.literal()));
        }
        if (match(TokenType.STRING_LITERAL)) {
            return new Expr.LiteralExpr(Value.ofVarchar((String) tok.literal()));
        }
        if (match(TokenType.TRUE)) {
            return new Expr.LiteralExpr(Value.ofBoolean(true));
        }
        if (match(TokenType.FALSE)) {
            return new Expr.LiteralExpr(Value.ofBoolean(false));
        }
        if (match(TokenType.NULL)) {
            return new Expr.LiteralExpr(Value.NULL);
        }
        if (match(TokenType.LPAREN)) {
            Expr inner = parseExpression();
            consume(TokenType.RPAREN, "Expected ')' after parenthesized expression");
            return inner;
        }
        if (match(TokenType.IDENTIFIER)) {
            String first = tok.lexeme();
            if (match(TokenType.DOT)) {
                Token colTok = consumeIdentifier("Expected column name after '.'");
                return new Expr.ColumnRefExpr(first, colTok.lexeme());
            }
            return new Expr.ColumnRefExpr(null, first);
        }
        throw error(tok, "Expected expression, found '" + tok.lexeme() + "'");
    }

    private Token consumeIdentifier(String message) {
        return consume(TokenType.IDENTIFIER, message);
    }

    private Token consume(TokenType expected, String message) {
        if (check(expected)) {
            return advance();
        }
        throw error(peek(), message + " (found '" + peek().lexeme() + "')");
    }

    private boolean match(TokenType... types) {
        for (TokenType type : types) {
            if (check(type)) {
                advance();
                return true;
            }
        }
        return false;
    }

    private boolean check(TokenType type) {
        if (isAtEnd()) {
            return type == TokenType.EOF;
        }
        return peek().type() == type;
    }

    private Token advance() {
        if (!isAtEnd()) {
            current++;
        }
        return previous();
    }

    private boolean isAtEnd() {
        return peek().type() == TokenType.EOF;
    }

    private Token peek() {
        return tokens.get(current);
    }

    private Token previous() {
        return tokens.get(current - 1);
    }

    private ParseException error(Token token, String message) {
        return new ParseException(message, token);
    }
}
