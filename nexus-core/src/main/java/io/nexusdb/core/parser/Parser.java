package io.nexusdb.core.parser;

import io.nexusdb.core.ast.Assignment;
import io.nexusdb.core.ast.ColumnDef;
import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.ast.JoinClause;
import io.nexusdb.core.ast.JoinType;
import io.nexusdb.core.ast.OrderByItem;
import io.nexusdb.core.ast.SelectItem;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.ast.TableRef;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Recursive-descent SQL parser producing immutable AST nodes in {@code io.nexusdb.core.ast}.
 *
 * <p>Operator Precedence Invariant (lowest to highest, per {@code docs/sql-engine.md}):
 * <ol>
 *   <li>{@code OR} (left-associative)</li>
 *   <li>{@code AND} (left-associative)</li>
 *   <li>{@code NOT} (right-associative prefix)</li>
 *   <li>Comparison ({@code =}, {@code !=}, {@code <>}, {@code <}, {@code <=}, {@code >}, {@code >=},
 *       {@code IS NULL}, {@code IS NOT NULL}) (non-associative)</li>
 *   <li>Additive ({@code +}, {@code -}) (left-associative)</li>
 *   <li>Multiplicative ({@code *}, {@code /}) (left-associative)</li>
 *   <li>Unary negation ({@code -}) (right-associative prefix)</li>
 *   <li>Primary (literals, column refs {@code col} / {@code tbl.col}, parenthesized expressions)</li>
 * </ol>
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

    /**
     * Parses one or more semicolon-separated SQL statements from {@code sqlScript}.
     */
    public static List<Statement> parseScript(String sqlScript) {
        Objects.requireNonNull(sqlScript, "sqlScript must not be null");
        List<Token> tokens;
        try {
            tokens = Lexer.tokenize(sqlScript);
        } catch (LexerException e) {
            throw new ParseException(e.getMessage(), e.getLine(), e.getColumn(), e);
        }
        return new Parser(tokens).parseStatements();
    }

    public List<Statement> parseStatements() {
        while (match(TokenType.SEMICOLON)) {
            // Skip leading empty semicolons
        }
        if (isAtEnd()) {
            throw error(peek(), "Expected SQL statement but found end of input");
        }
        List<Statement> statements = new ArrayList<>();
        while (!isAtEnd()) {
            statements.add(parseCoreStatement());
            if (!isAtEnd()) {
                consume(TokenType.SEMICOLON, "Expected ';' between SQL statements");
                while (match(TokenType.SEMICOLON)) {
                    // Skip consecutive semicolons
                }
            }
        }
        return List.copyOf(statements);
    }

    public static Expr parseExpr(String exprSql) {
        Objects.requireNonNull(exprSql, "exprSql must not be null");
        List<Token> tokens;
        try {
            tokens = Lexer.tokenize(exprSql);
        } catch (LexerException e) {
            throw new ParseException(e.getMessage(), e.getLine(), e.getColumn(), e);
        }
        Parser parser = new Parser(tokens);
        Expr expr = parser.parseExpression();
        if (!parser.isAtEnd()) {
            throw parser.error(parser.peek(), "Unexpected token '" + parser.peek().lexeme() + "' after expression");
        }
        return expr;
    }

    public Statement parseStatement() {
        if (isAtEnd()) {
            throw error(peek(), "Expected SQL statement but found end of input");
        }
        Statement stmt = parseCoreStatement();

        // Optional trailing semicolon
        match(TokenType.SEMICOLON);
        if (!isAtEnd()) {
            throw error(peek(), "Unexpected token '" + peek().lexeme() + "' after end of statement");
        }
        return stmt;
    }

    private Statement parseCoreStatement() {
        return switch (peek().type()) {
            case EXPLAIN -> parseExplain();
            case CREATE -> parseCreate();
            case DROP -> parseDrop();
            case INSERT -> parseInsert();
            case SELECT -> parseSelect();
            case UPDATE -> parseUpdate();
            case DELETE -> parseDelete();
            case BEGIN -> {
                consume(TokenType.BEGIN, "Expected 'BEGIN'");
                yield new Statement.BeginStatement();
            }
            case COMMIT -> {
                consume(TokenType.COMMIT, "Expected 'COMMIT'");
                yield new Statement.CommitStatement();
            }
            case ROLLBACK -> {
                consume(TokenType.ROLLBACK, "Expected 'ROLLBACK'");
                yield new Statement.RollbackStatement();
            }
            default -> throw error(peek(), "Unexpected token '" + peek().lexeme() + "' at start of statement");
        };
    }

    private Statement.ExplainStatement parseExplain() {
        consume(TokenType.EXPLAIN, "Expected 'EXPLAIN'");
        boolean analyze = match(TokenType.ANALYZE);
        if (check(TokenType.EXPLAIN)) {
            throw error(peek(), "Cannot nest EXPLAIN inside EXPLAIN");
        }
        if (isAtEnd() || check(TokenType.SEMICOLON)) {
            throw error(peek(), "Expected SQL statement after EXPLAIN");
        }
        Statement target = parseCoreStatement();
        return new Statement.ExplainStatement(analyze, target);
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
        boolean unique = match(TokenType.UNIQUE);
        if (match(TokenType.INDEX)) {
            return parseCreateIndex(unique);
        }
        if (unique) {
            throw error(peek(), "Expected 'INDEX' after 'CREATE UNIQUE', found '" + peek().lexeme() + "'");
        }
        throw error(peek(), "Expected 'DATABASE', 'TABLE', 'INDEX', or 'UNIQUE INDEX' after 'CREATE', found '"
                + peek().lexeme() + "'");
    }

    private Statement.CreateIndexStatement parseCreateIndex(boolean unique) {
        Token indexName = consumeIdentifier("Expected index name after 'INDEX'");
        consume(TokenType.ON, "Expected 'ON' after index name '" + indexName.lexeme() + "'");
        Token tableName = consumeIdentifier("Expected table name after 'ON'");
        consume(TokenType.LPAREN, "Expected '(' after table name '" + tableName.lexeme() + "'");
        Token columnName = consumeIdentifier("Expected column name in CREATE INDEX");
        consume(TokenType.RPAREN, "Expected ')' after column name '" + columnName.lexeme() + "'");
        return new Statement.CreateIndexStatement(
                indexName.lexeme(), tableName.lexeme(), columnName.lexeme(), unique);
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
        if (match(TokenType.INDEX)) {
            Token indexName = consumeIdentifier("Expected index name after 'DROP INDEX'");
            String tableName = null;
            if (match(TokenType.ON)) {
                Token tableTok = consumeIdentifier("Expected table name after 'ON'");
                tableName = tableTok.lexeme();
            }
            return new Statement.DropIndexStatement(indexName.lexeme(), tableName);
        }
        throw error(peek(), "Expected 'TABLE' or 'INDEX' after 'DROP', found '" + peek().lexeme() + "'");
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

    private Statement.SelectStatement parseSelect() {
        consume(TokenType.SELECT, "Expected 'SELECT'");
        if (check(TokenType.FROM) || isAtEnd()) {
            throw error(peek(), "Expected select projection list after 'SELECT'");
        }

        List<SelectItem> selectItems = new ArrayList<>();
        do {
            selectItems.add(parseSelectItem());
        } while (match(TokenType.COMMA));

        consume(TokenType.FROM, "Expected 'FROM' after SELECT list");
        TableRef from = parseTableRef();

        List<JoinClause> joins = new ArrayList<>();
        while (check(TokenType.JOIN) || check(TokenType.INNER) || check(TokenType.LEFT)) {
            joins.add(parseJoinClause());
        }

        Expr where = null;
        if (match(TokenType.WHERE)) {
            where = parseExpression();
        }

        List<Expr> groupBy = new ArrayList<>();
        if (match(TokenType.GROUP)) {
            consume(TokenType.BY, "Expected 'BY' after 'GROUP'");
            do {
                groupBy.add(parseExpression());
            } while (match(TokenType.COMMA));
        }

        Expr having = null;
        if (match(TokenType.HAVING)) {
            having = parseExpression();
        }

        List<OrderByItem> orderBy = new ArrayList<>();
        if (match(TokenType.ORDER)) {
            consume(TokenType.BY, "Expected 'BY' after 'ORDER'");
            do {
                Expr sortExpr = parseExpression();
                boolean ascending = true;
                if (match(TokenType.ASC)) {
                    ascending = true;
                } else if (match(TokenType.DESC)) {
                    ascending = false;
                }
                orderBy.add(new OrderByItem(sortExpr, ascending));
            } while (match(TokenType.COMMA));
        }

        Integer limit = null;
        if (match(TokenType.LIMIT)) {
            Token limitTok = consume(TokenType.INTEGER_LITERAL, "Expected non-negative integer literal after 'LIMIT'");
            long rawLimit = (Long) limitTok.literal();
            if (rawLimit < 0 || rawLimit > Integer.MAX_VALUE) {
                throw error(limitTok, "LIMIT out of range: " + rawLimit);
            }
            limit = (int) rawLimit;
        }

        return new Statement.SelectStatement(selectItems, from, joins, where, groupBy, having, orderBy, limit);
    }

    private JoinClause parseJoinClause() {
        JoinType joinType;
        if (match(TokenType.INNER)) {
            consume(TokenType.JOIN, "Expected 'JOIN' after 'INNER'");
            joinType = JoinType.INNER;
        } else if (match(TokenType.LEFT)) {
            match(TokenType.OUTER);
            consume(TokenType.JOIN, "Expected 'JOIN' after 'LEFT'");
            joinType = JoinType.LEFT;
        } else {
            consume(TokenType.JOIN, "Expected 'JOIN'");
            joinType = JoinType.INNER;
        }
        TableRef rightTable = parseTableRef();
        consume(TokenType.ON, "Expected 'ON' after joined table '" + rightTable.effectiveName() + "'");
        Expr condition = parseExpression();
        return new JoinClause(joinType, rightTable, condition);
    }

    private SelectItem parseSelectItem() {
        if (match(TokenType.STAR)) {
            return new SelectItem.AllColumns(null);
        }
        if (check(TokenType.IDENTIFIER)
                && checkAt(1, TokenType.DOT)
                && checkAt(2, TokenType.STAR)) {
            Token qualifier = advance();
            advance(); // DOT
            advance(); // STAR
            return new SelectItem.AllColumns(qualifier.lexeme());
        }

        Expr expr = parseExpression();
        String alias = null;
        if (match(TokenType.AS)) {
            Token aliasTok = consumeIdentifier("Expected alias identifier after 'AS'");
            alias = aliasTok.lexeme();
        } else if (check(TokenType.IDENTIFIER)) {
            alias = advance().lexeme();
        }
        return new SelectItem.ExprItem(expr, alias);
    }

    private TableRef parseTableRef() {
        Token tableTok = consumeIdentifier("Expected table name after 'FROM'");
        String alias = null;
        if (match(TokenType.AS)) {
            Token aliasTok = consumeIdentifier("Expected table alias identifier after 'AS'");
            alias = aliasTok.lexeme();
        } else if (check(TokenType.IDENTIFIER)) {
            alias = advance().lexeme();
        }
        return new TableRef(tableTok.lexeme(), alias);
    }

    private Statement.UpdateStatement parseUpdate() {
        consume(TokenType.UPDATE, "Expected 'UPDATE'");
        Token tableTok = consumeIdentifier("Expected table name after 'UPDATE'");
        consume(TokenType.SET, "Expected 'SET' after table name in UPDATE");

        List<Assignment> assignments = new ArrayList<>();
        do {
            Token colTok = consumeIdentifier("Expected column name in SET assignment");
            consume(TokenType.EQ, "Expected '=' after column name '" + colTok.lexeme() + "' in SET clause");
            Expr valExpr = parseExpression();
            assignments.add(new Assignment(colTok.lexeme(), valExpr));
        } while (match(TokenType.COMMA));

        Expr where = null;
        if (match(TokenType.WHERE)) {
            where = parseExpression();
        }
        return new Statement.UpdateStatement(tableTok.lexeme(), assignments, where);
    }

    private Statement.DeleteStatement parseDelete() {
        consume(TokenType.DELETE, "Expected 'DELETE'");
        consume(TokenType.FROM, "Expected 'FROM' after 'DELETE'");
        Token tableTok = consumeIdentifier("Expected table name after 'DELETE FROM'");

        Expr where = null;
        if (match(TokenType.WHERE)) {
            where = parseExpression();
        }
        return new Statement.DeleteStatement(tableTok.lexeme(), where);
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
        if (match(TokenType.COUNT, TokenType.SUM, TokenType.AVG, TokenType.MIN, TokenType.MAX)) {
            return parseAggregateCall(previous());
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

    private Expr.AggregateCall parseAggregateCall(Token funcToken) {
        Expr.AggregateFunc func = switch (funcToken.type()) {
            case COUNT -> Expr.AggregateFunc.COUNT;
            case SUM -> Expr.AggregateFunc.SUM;
            case AVG -> Expr.AggregateFunc.AVG;
            case MIN -> Expr.AggregateFunc.MIN;
            case MAX -> Expr.AggregateFunc.MAX;
            default -> throw error(funcToken, "Unexpected aggregate function: " + funcToken.lexeme());
        };
        consume(TokenType.LPAREN, "Expected '(' after aggregate function '" + funcToken.lexeme() + "'");
        if (match(TokenType.STAR)) {
            if (func != Expr.AggregateFunc.COUNT) {
                throw error(previous(), func.name() + "(*) is not supported; only COUNT(*) is valid");
            }
            consume(TokenType.RPAREN, "Expected ')' after 'COUNT(*'");
            return Expr.AggregateCall.countStar();
        }
        Expr arg = parseExpression();
        consume(TokenType.RPAREN, "Expected ')' after aggregate argument in " + func.name() + "(...)");
        return Expr.AggregateCall.of(func, arg);
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

    private boolean checkAt(int offset, TokenType type) {
        int idx = current + offset;
        if (idx >= tokens.size()) {
            return false;
        }
        return tokens.get(idx).type() == type;
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
