package io.nexusdb.core.planner;

import io.nexusdb.core.ast.Assignment;
import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.ColumnDef;
import io.nexusdb.core.ast.Expr;
import io.nexusdb.core.ast.JoinClause;
import io.nexusdb.core.ast.OrderByItem;
import io.nexusdb.core.ast.SelectItem;
import io.nexusdb.core.ast.Statement;
import io.nexusdb.core.ast.TableRef;
import io.nexusdb.core.catalog.Catalog;
import io.nexusdb.core.catalog.IndexMetadata;
import io.nexusdb.core.catalog.TableMetadata;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.DataType;
import io.nexusdb.core.record.Schema;
import io.nexusdb.core.record.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Semantic analyzer and type checker that binds parsed AST {@link Statement}s against {@link Catalog}.
 *
 * <p>Coercion & Validation Rules (per {@code docs/sql-engine.md}):
 * <ul>
 *   <li>Implicit numeric widening is permitted along {@code INT -> BIGINT -> DOUBLE}.</li>
 *   <li>Implicit narrowing (e.g. {@code DOUBLE -> INT}, {@code BIGINT -> INT}) and cross-category
 *       conversions (e.g. {@code VARCHAR <-> INT}, {@code BOOLEAN <-> INT}) are rejected with {@link BindException}.</li>
 *   <li>{@code VARCHAR(n)} string literals exceeding {@code n} characters and {@code NULL} values assigned
 *       to {@code NOT NULL} columns are rejected at bind time.</li>
 * </ul>
 */
public final class Binder {
    private final Catalog catalog;

    public Binder(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog must not be null");
    }

    public BoundStatement bind(Statement statement) {
        Objects.requireNonNull(statement, "statement must not be null");
        return switch (statement) {
            case Statement.CreateDatabaseStatement createDb -> bindCreateDatabase(createDb);
            case Statement.CreateTableStatement createTable -> bindCreateTable(createTable);
            case Statement.DropTableStatement dropTable -> bindDropTable(dropTable);
            case Statement.CreateIndexStatement createIndex -> bindCreateIndex(createIndex);
            case Statement.DropIndexStatement dropIndex -> bindDropIndex(dropIndex);
            case Statement.InsertStatement insert -> bindInsert(insert);
            case Statement.SelectStatement select -> bindSelect(select);
            case Statement.UpdateStatement update -> bindUpdate(update);
            case Statement.DeleteStatement delete -> bindDelete(delete);
            case Statement.BeginStatement ignored -> new BoundStatement.BoundBegin();
            case Statement.CommitStatement ignored -> new BoundStatement.BoundCommit();
            case Statement.RollbackStatement ignored -> new BoundStatement.BoundRollback();
            case Statement.ExplainStatement explain ->
                    new BoundStatement.BoundExplain(explain.analyze(), bind(explain.statement()));
        };
    }

    private BoundStatement.BoundCreateDatabase bindCreateDatabase(Statement.CreateDatabaseStatement stmt) {
        validateIdentifierOrThrow(stmt.databaseName(), "database name");
        return new BoundStatement.BoundCreateDatabase(stmt.databaseName());
    }

    private BoundStatement.BoundCreateTable bindCreateTable(Statement.CreateTableStatement stmt) {
        String tableName = stmt.tableName();
        validateIdentifierOrThrow(tableName, "table name");
        if (catalog.hasTable(tableName)) {
            throw new BindException("Table already exists: " + tableName);
        }

        Set<String> seenColumns = new HashSet<>();
        int primaryKeyCount = 0;
        List<Column> columns = new ArrayList<>(stmt.columns().size());

        for (ColumnDef def : stmt.columns()) {
            validateIdentifierOrThrow(def.name(), "column name");
            String norm = normalize(def.name());
            if (!seenColumns.add(norm)) {
                throw new BindException("Duplicate column name '" + def.name() + "' in CREATE TABLE '" + tableName + "'");
            }
            if (def.primaryKey()) {
                primaryKeyCount++;
                if (primaryKeyCount > 1) {
                    throw new BindException("Multiple PRIMARY KEY columns are not supported in table '" + tableName + "'");
                }
            }
            columns.add(new Column(def.name(), def.dataType(), def.maxLength(), def.nullable(), def.primaryKey()));
        }

        return new BoundStatement.BoundCreateTable(tableName, new Schema(columns));
    }

    private BoundStatement.BoundDropTable bindDropTable(Statement.DropTableStatement stmt) {
        TableMetadata meta = requireTable(stmt.tableName());
        return new BoundStatement.BoundDropTable(meta.getName());
    }

    private BoundStatement.BoundCreateIndex bindCreateIndex(Statement.CreateIndexStatement stmt) {
        validateIdentifierOrThrow(stmt.indexName(), "index name");
        validateIdentifierOrThrow(stmt.tableName(), "table name");
        validateIdentifierOrThrow(stmt.columnName(), "column name");

        TableMetadata table = requireTable(stmt.tableName());
        if (!table.getSchema().hasColumn(stmt.columnName())) {
            throw new BindException(
                    "Unknown column '" + stmt.columnName() + "' in table '" + table.getName() + "'");
        }
        if (catalog.findIndex(stmt.indexName()) != null) {
            throw new BindException("Index already exists: " + stmt.indexName());
        }
        Column canonicalCol = table.getSchema().getColumn(stmt.columnName());
        return new BoundStatement.BoundCreateIndex(
                stmt.indexName(), table.getName(), canonicalCol.name(), stmt.unique());
    }

    private BoundStatement.BoundDropIndex bindDropIndex(Statement.DropIndexStatement stmt) {
        validateIdentifierOrThrow(stmt.indexName(), "index name");
        if (stmt.tableName() != null) {
            validateIdentifierOrThrow(stmt.tableName(), "table name");
            TableMetadata table = requireTable(stmt.tableName());
            IndexMetadata idx = table.getIndex(stmt.indexName());
            if (idx == null) {
                throw new BindException(
                        "Unknown index '" + stmt.indexName() + "' on table '" + table.getName() + "'");
            }
            return new BoundStatement.BoundDropIndex(idx.indexName(), table.getName());
        }
        IndexMetadata found = catalog.findIndex(stmt.indexName());
        if (found == null) {
            throw new BindException("Unknown index: " + stmt.indexName());
        }
        return new BoundStatement.BoundDropIndex(found.indexName(), found.tableName());
    }

    private BoundStatement.BoundInsert bindInsert(Statement.InsertStatement stmt) {
        TableMetadata table = requireTable(stmt.tableName());
        Schema schema = table.getSchema();
        int schemaColCount = schema.getColumnCount();

        int[] targetColumnIndexes;
        if (stmt.columns().isEmpty()) {
            targetColumnIndexes = new int[schemaColCount];
            for (int i = 0; i < schemaColCount; i++) {
                targetColumnIndexes[i] = i;
            }
        } else {
            List<String> specifiedCols = stmt.columns();
            targetColumnIndexes = new int[specifiedCols.size()];
            Set<Integer> specifiedSet = new HashSet<>();
            for (int i = 0; i < specifiedCols.size(); i++) {
                String colName = specifiedCols.get(i);
                int colIdx = schema.getColumnIndex(colName);
                if (colIdx < 0) {
                    throw new BindException("Unknown column '" + colName + "' in table '" + table.getName() + "'");
                }
                if (!specifiedSet.add(colIdx)) {
                    throw new BindException("Duplicate column '" + colName + "' in INSERT column list");
                }
                targetColumnIndexes[i] = colIdx;
            }
            // Verify omitted columns are nullable
            for (int i = 0; i < schemaColCount; i++) {
                if (!specifiedSet.contains(i)) {
                    Column col = schema.getColumn(i);
                    if (!col.nullable()) {
                        throw new BindException(
                                "Missing value for NOT NULL column '" + col.name() + "' in INSERT INTO '" + table.getName() + "'");
                    }
                }
            }
        }

        List<List<BoundExpr>> boundRows = new ArrayList<>(stmt.rows().size());
        for (int rowIdx = 0; rowIdx < stmt.rows().size(); rowIdx++) {
            List<Expr> rawRow = stmt.rows().get(rowIdx);
            if (rawRow.size() != targetColumnIndexes.length) {
                throw new BindException(
                        "INSERT row " + (rowIdx + 1) + " has " + rawRow.size()
                                + " values, but expected " + targetColumnIndexes.length);
            }

            BoundExpr[] alignedRow = new BoundExpr[schemaColCount];
            for (int i = 0; i < targetColumnIndexes.length; i++) {
                int schemaIdx = targetColumnIndexes[i];
                Column targetCol = schema.getColumn(schemaIdx);
                BoundExpr boundVal = bindConstantOrRowExpr(rawRow.get(i), null, null);
                alignedRow[schemaIdx] = coerceForAssignment(boundVal, targetCol);
            }
            for (int colIdx = 0; colIdx < schemaColCount; colIdx++) {
                if (alignedRow[colIdx] == null) {
                    Column col = schema.getColumn(colIdx);
                    alignedRow[colIdx] = new BoundExpr.BoundLiteral(Value.NULL, col.type());
                }
            }
            boundRows.add(List.of(alignedRow));
        }

        return new BoundStatement.BoundInsert(table, boundRows);
    }

    private record TableScope(TableMetadata table, String alias, int startColIdx) {
        String effectiveName() {
            return alias != null ? alias : table.getName();
        }
    }

    private BoundStatement.BoundSelect bindSelect(Statement.SelectStatement stmt) {
        TableRef from = stmt.from();
        TableMetadata baseTable = requireTable(from.tableName());
        String baseAlias = from.alias();

        List<TableScope> scopes = new ArrayList<>(1 + stmt.joins().size());
        Set<String> seenEffectiveNames = new HashSet<>();
        TableScope baseScope = new TableScope(baseTable, baseAlias, 0);
        scopes.add(baseScope);
        seenEffectiveNames.add(normalize(baseScope.effectiveName()));

        int currentOffset = baseTable.getSchema().getColumnCount();
        List<BoundStatement.BoundJoin> boundJoins = new ArrayList<>(stmt.joins().size());

        for (JoinClause joinClause : stmt.joins()) {
            TableRef rightRef = joinClause.rightTable();
            TableMetadata rightTable = requireTable(rightRef.tableName());
            String rightAlias = rightRef.alias();
            TableScope rightScope = new TableScope(rightTable, rightAlias, currentOffset);
            String normEff = normalize(rightScope.effectiveName());
            if (!seenEffectiveNames.add(normEff)) {
                throw new BindException("Duplicate table name or alias '" + rightScope.effectiveName() + "' in FROM/JOIN");
            }
            scopes.add(rightScope);
            currentOffset += rightTable.getSchema().getColumnCount();

            BoundExpr boundOn = bindJoinOnPredicate(joinClause.condition(), scopes);
            boundJoins.add(new BoundStatement.BoundJoin(
                    joinClause.joinType(),
                    rightTable,
                    rightAlias,
                    boundOn,
                    rightScope.startColIdx()));
        }

        BoundExpr boundWhere = null;
        if (stmt.where() != null) {
            boundWhere = bindWherePredicate(stmt.where(), scopes);
        }

        List<BoundExpr> boundGroupBy = new ArrayList<>(stmt.groupBy().size());
        for (Expr gExpr : stmt.groupBy()) {
            boundGroupBy.add(bindExprWithScopes(gExpr, scopes, false));
        }

        boolean hasAggregateInQuery = !boundGroupBy.isEmpty() || stmt.having() != null;

        List<BoundStatement.BoundProjection> rawProjections = new ArrayList<>();
        List<Column> outputColumns = new ArrayList<>();
        Map<String, BoundExpr> aliasMap = new HashMap<>();
        Set<String> ambiguousAliases = new HashSet<>();

        for (SelectItem item : stmt.selectItems()) {
            switch (item) {
                case SelectItem.AllColumns(String qualifier) -> {
                    if (qualifier != null) {
                        TableScope targetScope = resolveQualifierScope(qualifier, scopes);
                        Schema s = targetScope.table().getSchema();
                        for (int i = 0; i < s.getColumnCount(); i++) {
                            Column col = s.getColumn(i);
                            int globalIdx = targetScope.startColIdx() + i;
                            String colLabel = scopes.size() > 1
                                    ? targetScope.effectiveName() + "." + col.name()
                                    : col.name();
                            BoundExpr colRef = new BoundExpr.BoundColumnRef(globalIdx, colLabel, col.type());
                            rawProjections.add(new BoundStatement.BoundProjection(colRef, col.name()));
                            outputColumns.add(
                                    new Column(uniqueOutputColumnName(col.name(), outputColumns), col.type(), col.maxLength(), true, false));
                        }
                    } else {
                        for (TableScope scope : scopes) {
                            Schema s = scope.table().getSchema();
                            for (int i = 0; i < s.getColumnCount(); i++) {
                                Column col = s.getColumn(i);
                                int globalIdx = scope.startColIdx() + i;
                                String colLabel = scopes.size() > 1
                                        ? scope.effectiveName() + "." + col.name()
                                        : col.name();
                                BoundExpr colRef = new BoundExpr.BoundColumnRef(globalIdx, colLabel, col.type());
                                rawProjections.add(new BoundStatement.BoundProjection(colRef, col.name()));
                                outputColumns.add(
                                        new Column(uniqueOutputColumnName(col.name(), outputColumns), col.type(), col.maxLength(), true, false));
                            }
                        }
                    }
                }
                case SelectItem.ExprItem(Expr expr, String alias) -> {
                    BoundExpr boundExpr = bindExprWithScopes(expr, scopes, true);
                    if (containsAggregate(boundExpr)) {
                        hasAggregateInQuery = true;
                    }
                    String outName = alias != null ? alias : deriveColumnName(expr);
                    DataType outType = boundExpr.dataType() != null ? boundExpr.dataType() : DataType.INT;
                    int maxLen = outType == DataType.VARCHAR ? resolveVarcharMaxLength(boundExpr, scopes) : outType.getFixedSize();
                    rawProjections.add(new BoundStatement.BoundProjection(boundExpr, outName));
                    outputColumns.add(
                            new Column(uniqueOutputColumnName(outName, outputColumns), outType, maxLen, true, false));

                    if (alias != null) {
                        String normAlias = normalize(alias);
                        if (aliasMap.containsKey(normAlias)) {
                            ambiguousAliases.add(normAlias);
                        } else {
                            aliasMap.put(normAlias, boundExpr);
                        }
                    }
                }
            }
        }

        BoundExpr rawHaving = null;
        if (stmt.having() != null) {
            rawHaving = bindHavingPredicate(stmt.having(), scopes);
            if (containsAggregate(rawHaving)) {
                hasAggregateInQuery = true;
            }
        }

        if (stmt.having() != null && boundGroupBy.isEmpty() && !hasAggregateInQuery) {
            for (BoundStatement.BoundProjection proj : rawProjections) {
                if (containsAggregate(proj.expr())) {
                    hasAggregateInQuery = true;
                    break;
                }
            }
            if (!hasAggregateInQuery) {
                throw new BindException("HAVING clause requires a GROUP BY clause or aggregate functions");
            }
        }

        List<BoundStatement.BoundOrderBy> rawOrderBy = new ArrayList<>(stmt.orderBy().size());
        for (OrderByItem item : stmt.orderBy()) {
            BoundExpr sortExpr;
            if (item.expr() instanceof Expr.ColumnRefExpr(String qualifier, String colName)
                    && qualifier == null
                    && !hasUnqualifiedColumn(colName, scopes)) {
                String norm = normalize(colName);
                if (ambiguousAliases.contains(norm)) {
                    throw new BindException("Ambiguous SELECT alias '" + colName + "' in ORDER BY");
                }
                BoundExpr aliased = aliasMap.get(norm);
                if (aliased == null) {
                    throw new BindException("Unknown column '" + colName + "' in ORDER BY");
                }
                sortExpr = aliased;
            } else if (item.expr() instanceof Expr.ColumnRefExpr(String qualifier, String colName)
                    && qualifier == null
                    && aliasMap.containsKey(normalize(colName))
                    && !ambiguousAliases.contains(normalize(colName))) {
                BoundExpr aliased = aliasMap.get(normalize(colName));
                if (hasAggregateInQuery && containsAggregate(aliased)) {
                    sortExpr = aliased;
                } else {
                    sortExpr = bindExprWithScopes(item.expr(), scopes, true);
                }
            } else {
                sortExpr = bindExprWithScopes(item.expr(), scopes, true);
            }
            if (containsAggregate(sortExpr)) {
                hasAggregateInQuery = true;
            }
            rawOrderBy.add(new BoundStatement.BoundOrderBy(sortExpr, item.ascending()));
        }

        List<BoundStatement.BoundProjection> finalProjections;
        List<BoundExpr.BoundAggregate> aggregates = new ArrayList<>();
        BoundExpr finalHaving = null;
        List<BoundStatement.BoundOrderBy> finalOrderBy;

        if (hasAggregateInQuery) {
            finalProjections = new ArrayList<>(rawProjections.size());
            for (BoundStatement.BoundProjection proj : rawProjections) {
                BoundExpr rewritten = rewritePostAggregateExpr(proj.expr(), boundGroupBy, aggregates, "SELECT");
                finalProjections.add(new BoundStatement.BoundProjection(rewritten, proj.alias()));
            }
            if (rawHaving != null) {
                finalHaving = rewritePostAggregateExpr(rawHaving, boundGroupBy, aggregates, "HAVING");
            }
            finalOrderBy = new ArrayList<>(rawOrderBy.size());
            for (BoundStatement.BoundOrderBy ob : rawOrderBy) {
                BoundExpr rewrittenSort = rewritePostAggregateExpr(ob.expr(), boundGroupBy, aggregates, "ORDER BY");
                finalOrderBy.add(new BoundStatement.BoundOrderBy(rewrittenSort, ob.ascending()));
            }
        } else {
            finalProjections = rawProjections;
            finalOrderBy = rawOrderBy;
        }

        Schema outputSchema = new Schema(outputColumns);
        return new BoundStatement.BoundSelect(
                baseTable,
                baseAlias,
                boundJoins,
                finalProjections,
                outputSchema,
                boundWhere,
                boundGroupBy,
                aggregates,
                finalHaving,
                finalOrderBy,
                stmt.limit());
    }

    private BoundStatement.BoundUpdate bindUpdate(Statement.UpdateStatement stmt) {
        TableMetadata table = requireTable(stmt.tableName());
        Schema schema = table.getSchema();

        Set<Integer> assignedColumns = new HashSet<>();
        List<BoundStatement.BoundAssignment> boundAssignments = new ArrayList<>(stmt.assignments().size());

        for (Assignment assignment : stmt.assignments()) {
            int colIdx = schema.getColumnIndex(assignment.columnName());
            if (colIdx < 0) {
                throw new BindException(
                        "Unknown column '" + assignment.columnName() + "' in UPDATE of table '" + table.getName() + "'");
            }
            if (!assignedColumns.add(colIdx)) {
                throw new BindException(
                        "Duplicate assignment to column '" + assignment.columnName() + "' in UPDATE");
            }
            Column targetCol = schema.getColumn(colIdx);
            BoundExpr boundVal = bindConstantOrRowExpr(assignment.value(), table, null);
            BoundExpr coercedVal = coerceForAssignment(boundVal, targetCol);
            boundAssignments.add(new BoundStatement.BoundAssignment(colIdx, targetCol.name(), coercedVal));
        }

        BoundExpr boundWhere = null;
        if (stmt.where() != null) {
            boundWhere = bindWherePredicate(stmt.where(), table, null);
        }

        return new BoundStatement.BoundUpdate(table, boundAssignments, boundWhere);
    }

    private BoundStatement.BoundDelete bindDelete(Statement.DeleteStatement stmt) {
        TableMetadata table = requireTable(stmt.tableName());
        BoundExpr boundWhere = null;
        if (stmt.where() != null) {
            boundWhere = bindWherePredicate(stmt.where(), table, null);
        }
        return new BoundStatement.BoundDelete(table, boundWhere);
    }

    private BoundExpr bindWherePredicate(Expr whereExpr, TableMetadata table, String tableAlias) {
        List<TableScope> scopes = table == null ? List.of() : List.of(new TableScope(table, tableAlias, 0));
        return bindWherePredicate(whereExpr, scopes);
    }

    private BoundExpr bindWherePredicate(Expr whereExpr, List<TableScope> scopes) {
        BoundExpr bound = bindExprWithScopes(whereExpr, scopes, false);
        DataType type = bound.dataType();
        if (type != null && type != DataType.BOOLEAN) {
            throw new BindException("WHERE clause must evaluate to BOOLEAN, got " + type);
        }
        if (type == null) {
            return new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN);
        }
        return bound;
    }

    private BoundExpr bindJoinOnPredicate(Expr onExpr, List<TableScope> scopes) {
        BoundExpr bound = bindExprWithScopes(onExpr, scopes, false);
        DataType type = bound.dataType();
        if (type != null && type != DataType.BOOLEAN) {
            throw new BindException("JOIN ON clause must evaluate to BOOLEAN, got " + type);
        }
        if (type == null) {
            return new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN);
        }
        return bound;
    }

    private BoundExpr bindHavingPredicate(Expr havingExpr, List<TableScope> scopes) {
        BoundExpr bound = bindExprWithScopes(havingExpr, scopes, true);
        DataType type = bound.dataType();
        if (type != null && type != DataType.BOOLEAN) {
            throw new BindException("HAVING clause must evaluate to BOOLEAN, got " + type);
        }
        if (type == null) {
            return new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN);
        }
        return bound;
    }

    private BoundExpr bindConstantOrRowExpr(Expr expr, TableMetadata table, String tableAlias) {
        List<TableScope> scopes = table == null ? List.of() : List.of(new TableScope(table, tableAlias, 0));
        return bindExprWithScopes(expr, scopes, false);
    }

    private BoundExpr bindExprWithScopes(
            Expr expr,
            List<TableScope> scopes,
            boolean allowAggregates) {
        return switch (expr) {
            case Expr.LiteralExpr(Value val) -> new BoundExpr.BoundLiteral(val, val.dataType());
            case Expr.ColumnRefExpr(String qualifier, String colName) -> {
                if (scopes.isEmpty()) {
                    throw new BindException("Column reference '" + colName + "' is not allowed in INSERT VALUES");
                }
                yield resolveColumnRef(qualifier, colName, scopes);
            }
            case Expr.UnaryExpr(Expr.UnaryOp op, Expr operand) -> {
                // Fold negative numeric literals directly (handles -2147483648 as INT)
                if (op == Expr.UnaryOp.NEGATE && operand instanceof Expr.LiteralExpr(Value litVal)) {
                    if (litVal instanceof Value.IntValue(int v)) {
                        yield new BoundExpr.BoundLiteral(Value.ofInt(-v), DataType.INT);
                    }
                    if (litVal instanceof Value.BigIntValue(long v)) {
                        long neg = -v;
                        if (neg >= Integer.MIN_VALUE && neg <= Integer.MAX_VALUE) {
                            yield new BoundExpr.BoundLiteral(Value.ofInt((int) neg), DataType.INT);
                        }
                        yield new BoundExpr.BoundLiteral(Value.ofBigInt(neg), DataType.BIGINT);
                    }
                    if (litVal instanceof Value.DoubleValue(double v)) {
                        yield new BoundExpr.BoundLiteral(Value.ofDouble(-v), DataType.DOUBLE);
                    }
                }

                BoundExpr boundOperand = bindExprWithScopes(operand, scopes, allowAggregates);
                DataType operandType = boundOperand.dataType();
                if (op == Expr.UnaryOp.NEGATE) {
                    if (operandType != null && !isNumeric(operandType)) {
                        throw new BindException("Unary '-' requires a numeric operand, got " + operandType);
                    }
                    DataType resultType = operandType != null ? operandType : DataType.INT;
                    yield new BoundExpr.BoundUnaryExpr(op, boundOperand, resultType);
                } else {
                    if (operandType != null && operandType != DataType.BOOLEAN) {
                        throw new BindException("Unary 'NOT' requires a BOOLEAN operand, got " + operandType);
                    }
                    yield new BoundExpr.BoundUnaryExpr(op, boundOperand, DataType.BOOLEAN);
                }
            }
            case Expr.BinaryExpr(Expr left, Expr.BinaryOp op, Expr right) -> {
                BoundExpr boundLeft = bindExprWithScopes(left, scopes, allowAggregates);
                BoundExpr boundRight = bindExprWithScopes(right, scopes, allowAggregates);
                yield bindBinaryOp(boundLeft, op, boundRight);
            }
            case Expr.IsNullExpr(Expr operand, boolean negated) -> {
                BoundExpr boundOperand = bindExprWithScopes(operand, scopes, allowAggregates);
                yield new BoundExpr.BoundIsNullExpr(boundOperand, negated);
            }
            case Expr.AggregateCall(Expr.AggregateFunc func, Expr arg, boolean star) -> {
                if (!allowAggregates) {
                    throw new BindException("Aggregate function '" + func.name()
                            + "' is not allowed in this clause (only SELECT, HAVING, and ORDER BY)");
                }
                if (star) {
                    yield new BoundExpr.BoundAggregate(Expr.AggregateFunc.COUNT, null, true, DataType.BIGINT);
                }
                if (containsAstAggregate(arg)) {
                    throw new BindException("Nested aggregate functions are not allowed in " + func.name() + "(...)");
                }
                BoundExpr boundArg = bindExprWithScopes(arg, scopes, false);
                DataType argType = boundArg.dataType();
                DataType returnType = switch (func) {
                    case COUNT -> DataType.BIGINT;
                    case SUM -> {
                        if (argType != null && !isNumeric(argType)) {
                            throw new BindException("Aggregate SUM requires a numeric argument, got " + argType);
                        }
                        yield argType == DataType.DOUBLE ? DataType.DOUBLE : DataType.BIGINT;
                    }
                    case AVG -> {
                        if (argType != null && !isNumeric(argType)) {
                            throw new BindException("Aggregate AVG requires a numeric argument, got " + argType);
                        }
                        yield DataType.DOUBLE;
                    }
                    case MIN, MAX -> {
                        if (argType == DataType.BOOLEAN) {
                            throw new BindException("Aggregate " + func.name() + " does not support BOOLEAN operand");
                        }
                        yield argType != null ? argType : DataType.INT;
                    }
                };
                yield new BoundExpr.BoundAggregate(func, boundArg, false, returnType);
            }
        };
    }

    private static BoundExpr.BoundColumnRef resolveColumnRef(
            String qualifier,
            String colName,
            List<TableScope> scopes) {
        if (qualifier != null) {
            TableScope scope = resolveQualifierScope(qualifier, scopes);
            Schema schema = scope.table().getSchema();
            int idx = schema.getColumnIndex(colName);
            if (idx < 0) {
                throw new BindException("Unknown column '" + qualifier + "." + colName + "' in table '"
                        + scope.table().getName() + "'");
            }
            Column col = schema.getColumn(idx);
            String label = scopes.size() > 1 ? scope.effectiveName() + "." + col.name() : col.name();
            return new BoundExpr.BoundColumnRef(scope.startColIdx() + idx, label, col.type());
        }

        TableScope matchedScope = null;
        int matchedLocalIdx = -1;
        for (TableScope scope : scopes) {
            int idx = scope.table().getSchema().getColumnIndex(colName);
            if (idx >= 0) {
                if (matchedScope != null) {
                    throw new BindException("Ambiguous column reference '" + colName
                            + "' (present in both '" + matchedScope.effectiveName()
                            + "' and '" + scope.effectiveName() + "')");
                }
                matchedScope = scope;
                matchedLocalIdx = idx;
            }
        }
        if (matchedScope == null) {
            throw new BindException("Unknown column '" + colName + "'");
        }
        Column col = matchedScope.table().getSchema().getColumn(matchedLocalIdx);
        String label = scopes.size() > 1 ? matchedScope.effectiveName() + "." + col.name() : col.name();
        return new BoundExpr.BoundColumnRef(matchedScope.startColIdx() + matchedLocalIdx, label, col.type());
    }

    private static boolean hasUnqualifiedColumn(String colName, List<TableScope> scopes) {
        for (TableScope scope : scopes) {
            if (scope.table().getSchema().hasColumn(colName)) {
                return true;
            }
        }
        return false;
    }

    private static TableScope resolveQualifierScope(String qualifier, List<TableScope> scopes) {
        for (TableScope scope : scopes) {
            if (scope.alias() != null && scope.alias().equalsIgnoreCase(qualifier)) {
                return scope;
            }
        }
        TableScope byTableName = null;
        for (TableScope scope : scopes) {
            if (scope.table().getName().equalsIgnoreCase(qualifier)) {
                if (byTableName != null) {
                    throw new BindException("Ambiguous table qualifier '" + qualifier + "'");
                }
                byTableName = scope;
            }
        }
        if (byTableName != null) {
            return byTableName;
        }
        throw new BindException("Unknown table qualifier '" + qualifier + "'");
    }

    private static boolean containsAstAggregate(Expr expr) {
        if (expr == null) {
            return false;
        }
        return switch (expr) {
            case Expr.AggregateCall ignored -> true;
            case Expr.LiteralExpr ignored -> false;
            case Expr.ColumnRefExpr ignored -> false;
            case Expr.UnaryExpr(var ignoredOp, Expr operand) -> containsAstAggregate(operand);
            case Expr.BinaryExpr(Expr left, var ignoredOp, Expr right) ->
                    containsAstAggregate(left) || containsAstAggregate(right);
            case Expr.IsNullExpr(Expr operand, boolean ignoredNeg) -> containsAstAggregate(operand);
        };
    }

    private static boolean containsAggregate(BoundExpr expr) {
        if (expr == null) {
            return false;
        }
        return switch (expr) {
            case BoundExpr.BoundAggregate ignored -> true;
            case BoundExpr.BoundLiteral ignored -> false;
            case BoundExpr.BoundColumnRef ignored -> false;
            case BoundExpr.BoundUnaryExpr(var ignoredOp, BoundExpr operand, var ignoredType) ->
                    containsAggregate(operand);
            case BoundExpr.BoundBinaryExpr(BoundExpr left, var ignoredOp, BoundExpr right, var ignoredType) ->
                    containsAggregate(left) || containsAggregate(right);
            case BoundExpr.BoundIsNullExpr(BoundExpr operand, boolean ignoredNeg) ->
                    containsAggregate(operand);
            case BoundExpr.BoundCastExpr(BoundExpr operand, var ignoredType) ->
                    containsAggregate(operand);
        };
    }

    private static BoundExpr rewritePostAggregateExpr(
            BoundExpr expr,
            List<BoundExpr> groupBy,
            List<BoundExpr.BoundAggregate> aggregates,
            String clauseName) {
        int groupIdx = groupBy.indexOf(expr);
        if (groupIdx >= 0) {
            String colName = expr instanceof BoundExpr.BoundColumnRef(int ignored, String name, var ignoredType)
                    ? name
                    : "group_" + groupIdx;
            DataType dt = expr.dataType() != null ? expr.dataType() : DataType.INT;
            return new BoundExpr.BoundColumnRef(groupIdx, colName, dt);
        }

        return switch (expr) {
            case BoundExpr.BoundAggregate agg -> {
                int aggIdx = aggregates.indexOf(agg);
                if (aggIdx < 0) {
                    aggIdx = aggregates.size();
                    aggregates.add(agg);
                }
                int slotIdx = groupBy.size() + aggIdx;
                yield new BoundExpr.BoundColumnRef(slotIdx, agg.displayText(), agg.dataType());
            }
            case BoundExpr.BoundColumnRef colRef ->
                    throw new BindException("Column '" + colRef.columnName() + "' in " + clauseName
                            + " must appear in the GROUP BY clause or be used in an aggregate function");
            case BoundExpr.BoundLiteral lit -> lit;
            case BoundExpr.BoundUnaryExpr(Expr.UnaryOp op, BoundExpr operand, DataType dt) ->
                    new BoundExpr.BoundUnaryExpr(
                            op,
                            rewritePostAggregateExpr(operand, groupBy, aggregates, clauseName),
                            dt);
            case BoundExpr.BoundBinaryExpr(BoundExpr left, Expr.BinaryOp op, BoundExpr right, DataType dt) ->
                    new BoundExpr.BoundBinaryExpr(
                            rewritePostAggregateExpr(left, groupBy, aggregates, clauseName),
                            op,
                            rewritePostAggregateExpr(right, groupBy, aggregates, clauseName),
                            dt);
            case BoundExpr.BoundIsNullExpr(BoundExpr operand, boolean negated) ->
                    new BoundExpr.BoundIsNullExpr(
                            rewritePostAggregateExpr(operand, groupBy, aggregates, clauseName),
                            negated);
            case BoundExpr.BoundCastExpr(BoundExpr operand, DataType dt) ->
                    new BoundExpr.BoundCastExpr(
                            rewritePostAggregateExpr(operand, groupBy, aggregates, clauseName),
                            dt);
        };
    }

    private BoundExpr bindBinaryOp(BoundExpr left, Expr.BinaryOp op, BoundExpr right) {
        DataType leftType = left.dataType();
        DataType rightType = right.dataType();

        return switch (op) {
            case AND, OR -> {
                if (leftType != null && leftType != DataType.BOOLEAN) {
                    throw new BindException("Logical operator '" + op.sql() + "' requires BOOLEAN operands, got " + leftType);
                }
                if (rightType != null && rightType != DataType.BOOLEAN) {
                    throw new BindException("Logical operator '" + op.sql() + "' requires BOOLEAN operands, got " + rightType);
                }
                BoundExpr coercedLeft = leftType == null ? new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN) : left;
                BoundExpr coercedRight = rightType == null ? new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN) : right;
                yield new BoundExpr.BoundBinaryExpr(coercedLeft, op, coercedRight, DataType.BOOLEAN);
            }
            case ADD, SUB, MUL, DIV -> {
                if (leftType != null && !isNumeric(leftType)) {
                    throw new BindException("Arithmetic operator '" + op.sql() + "' requires numeric operands, got " + leftType);
                }
                if (rightType != null && !isNumeric(rightType)) {
                    throw new BindException("Arithmetic operator '" + op.sql() + "' requires numeric operands, got " + rightType);
                }
                DataType commonType = widestNumeric(leftType, rightType);
                BoundExpr coercedLeft = coerceNumeric(left, commonType);
                BoundExpr coercedRight = coerceNumeric(right, commonType);
                yield new BoundExpr.BoundBinaryExpr(coercedLeft, op, coercedRight, commonType);
            }
            case EQ, NEQ, LT, LTE, GT, GTE -> {
                if (leftType != null && rightType != null) {
                    if (isNumeric(leftType) && isNumeric(rightType)) {
                        DataType commonType = widestNumeric(leftType, rightType);
                        yield new BoundExpr.BoundBinaryExpr(
                                coerceNumeric(left, commonType),
                                op,
                                coerceNumeric(right, commonType),
                                DataType.BOOLEAN);
                    }
                    if (leftType != rightType) {
                        throw new BindException(
                                "Cannot compare incompatible types " + leftType + " and " + rightType
                                        + " with '" + op.sql() + "'");
                    }
                }
                yield new BoundExpr.BoundBinaryExpr(left, op, right, DataType.BOOLEAN);
            }
        };
    }

    private BoundExpr coerceForAssignment(BoundExpr expr, Column targetCol) {
        DataType targetType = targetCol.type();
        DataType srcType = expr.dataType();

        if (expr instanceof BoundExpr.BoundLiteral(Value val, DataType ignored)) {
            if (val.isNull()) {
                if (!targetCol.nullable()) {
                    throw new BindException("Column '" + targetCol.name() + "' is NOT NULL");
                }
                return new BoundExpr.BoundLiteral(Value.NULL, targetType);
            }
            if (targetType == DataType.VARCHAR && val instanceof Value.VarcharValue(String s)) {
                if (s.length() > targetCol.maxLength()) {
                    throw new BindException(
                            "VARCHAR value length " + s.length() + " exceeds maximum length "
                                    + targetCol.maxLength() + " for column '" + targetCol.name() + "'");
                }
                return expr;
            }
        }

        if (srcType == null) {
            if (!targetCol.nullable()) {
                throw new BindException("Column '" + targetCol.name() + "' is NOT NULL");
            }
            return new BoundExpr.BoundLiteral(Value.NULL, targetType);
        }

        if (srcType == targetType) {
            return expr;
        }

        if (canWidenNumeric(srcType, targetType)) {
            return coerceNumeric(expr, targetType);
        }

        throw new BindException(
                "Type mismatch for column '" + targetCol.name() + "': expected " + targetType + ", got " + srcType);
    }

    public static boolean isNumeric(DataType type) {
        return type == DataType.INT || type == DataType.BIGINT || type == DataType.DOUBLE;
    }

    public static boolean canWidenNumeric(DataType from, DataType to) {
        if (from == to) {
            return true;
        }
        if (from == DataType.INT) {
            return to == DataType.BIGINT || to == DataType.DOUBLE;
        }
        if (from == DataType.BIGINT) {
            return to == DataType.DOUBLE;
        }
        return false;
    }

    public static DataType widestNumeric(DataType a, DataType b) {
        if (a == DataType.DOUBLE || b == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (a == DataType.BIGINT || b == DataType.BIGINT) {
            return DataType.BIGINT;
        }
        return DataType.INT;
    }

    private static BoundExpr coerceNumeric(BoundExpr expr, DataType targetType) {
        if (expr.dataType() == targetType) {
            return expr;
        }
        if (expr instanceof BoundExpr.BoundLiteral(Value val, DataType ignored)) {
            if (val.isNull()) {
                return new BoundExpr.BoundLiteral(Value.NULL, targetType);
            }
            if (val instanceof Value.IntValue(int v)) {
                if (targetType == DataType.BIGINT) {
                    return new BoundExpr.BoundLiteral(Value.ofBigInt(v), DataType.BIGINT);
                }
                if (targetType == DataType.DOUBLE) {
                    return new BoundExpr.BoundLiteral(Value.ofDouble(v), DataType.DOUBLE);
                }
            }
            if (val instanceof Value.BigIntValue(long v) && targetType == DataType.DOUBLE) {
                return new BoundExpr.BoundLiteral(Value.ofDouble(v), DataType.DOUBLE);
            }
        }
        return new BoundExpr.BoundCastExpr(expr, targetType);
    }

    private TableMetadata requireTable(String tableName) {
        TableMetadata meta = catalog.getTable(tableName);
        if (meta == null) {
            throw new BindException("Unknown table: " + tableName);
        }
        return meta;
    }

    private static void validateTableQualifier(String qualifier, TableMetadata table, String tableAlias) {
        if (tableAlias != null) {
            if (!qualifier.equalsIgnoreCase(tableAlias) && !qualifier.equalsIgnoreCase(table.getName())) {
                throw new BindException(
                        "Unknown table qualifier '" + qualifier + "' (expected alias '" + tableAlias + "')");
            }
        } else if (!qualifier.equalsIgnoreCase(table.getName())) {
            throw new BindException(
                    "Unknown table qualifier '" + qualifier + "' (expected '" + table.getName() + "')");
        }
    }

    private static void validateIdentifierOrThrow(String identifier, String kind) {
        try {
            Catalog.validateIdentifier(identifier, kind);
        } catch (IllegalArgumentException e) {
            throw new BindException(e.getMessage(), e);
        }
    }

    private static int resolveVarcharMaxLength(BoundExpr expr, List<TableScope> scopes) {
        if (expr instanceof BoundExpr.BoundColumnRef(int idx, String ignoredName, DataType ignoredType)) {
            for (TableScope scope : scopes) {
                int local = idx - scope.startColIdx();
                Schema s = scope.table().getSchema();
                if (local >= 0 && local < s.getColumnCount()) {
                    return s.getColumn(local).maxLength();
                }
            }
        }
        if (expr instanceof BoundExpr.BoundLiteral(Value.VarcharValue(String s), DataType ignoredType)) {
            return Math.max(1, s.length());
        }
        return 255;
    }

    private static String deriveColumnName(Expr expr) {
        if (expr instanceof Expr.ColumnRefExpr(String ignoredQualifier, String colName)) {
            return colName;
        }
        return AstPrinter.print(expr);
    }

    private static String uniqueOutputColumnName(String baseName, List<Column> existing) {
        String candidate = baseName.isBlank() ? "col" : baseName;
        int suffix = 1;
        while (containsColumnName(existing, candidate)) {
            suffix++;
            candidate = baseName + "_" + suffix;
        }
        return candidate;
    }

    private static boolean containsColumnName(List<Column> cols, String name) {
        for (Column col : cols) {
            if (col.name().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }

    private static String normalize(String s) {
        return s.toLowerCase(Locale.ROOT);
    }
}
