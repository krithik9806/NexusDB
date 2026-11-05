package io.nexusdb.core.planner;

import io.nexusdb.core.ast.Assignment;
import io.nexusdb.core.ast.AstPrinter;
import io.nexusdb.core.ast.ColumnDef;
import io.nexusdb.core.ast.Expr;
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

    private BoundStatement.BoundSelect bindSelect(Statement.SelectStatement stmt) {
        TableRef from = stmt.from();
        TableMetadata table = requireTable(from.tableName());
        Schema tableSchema = table.getSchema();
        String tableAlias = from.alias();

        List<BoundStatement.BoundProjection> projections = new ArrayList<>();
        List<Column> outputColumns = new ArrayList<>();
        Map<String, BoundExpr> aliasMap = new HashMap<>();
        Set<String> ambiguousAliases = new HashSet<>();

        for (SelectItem item : stmt.selectItems()) {
            switch (item) {
                case SelectItem.AllColumns(String qualifier) -> {
                    if (qualifier != null) {
                        validateTableQualifier(qualifier, table, tableAlias);
                    }
                    for (int i = 0; i < tableSchema.getColumnCount(); i++) {
                        Column col = tableSchema.getColumn(i);
                        BoundExpr colRef = new BoundExpr.BoundColumnRef(i, col.name(), col.type());
                        projections.add(new BoundStatement.BoundProjection(colRef, col.name()));
                        outputColumns.add(
                                new Column(uniqueOutputColumnName(col.name(), outputColumns), col.type(), col.maxLength(), col.nullable(), false));
                    }
                }
                case SelectItem.ExprItem(Expr expr, String alias) -> {
                    BoundExpr boundExpr = bindConstantOrRowExpr(expr, table, tableAlias);
                    String outName = alias != null ? alias : deriveColumnName(expr);
                    DataType outType = boundExpr.dataType() != null ? boundExpr.dataType() : DataType.INT;
                    int maxLen = outType == DataType.VARCHAR ? resolveVarcharMaxLength(boundExpr, tableSchema) : outType.getFixedSize();
                    projections.add(new BoundStatement.BoundProjection(boundExpr, outName));
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

        BoundExpr boundWhere = null;
        if (stmt.where() != null) {
            boundWhere = bindWherePredicate(stmt.where(), table, tableAlias);
        }

        List<BoundStatement.BoundOrderBy> boundOrderBy = new ArrayList<>(stmt.orderBy().size());
        for (OrderByItem item : stmt.orderBy()) {
            BoundExpr sortExpr;
            if (item.expr() instanceof Expr.ColumnRefExpr(String qualifier, String colName)
                    && qualifier == null
                    && !tableSchema.hasColumn(colName)) {
                String norm = normalize(colName);
                if (ambiguousAliases.contains(norm)) {
                    throw new BindException("Ambiguous SELECT alias '" + colName + "' in ORDER BY");
                }
                BoundExpr aliased = aliasMap.get(norm);
                if (aliased == null) {
                    throw new BindException("Unknown column '" + colName + "' in ORDER BY");
                }
                sortExpr = aliased;
            } else {
                sortExpr = bindConstantOrRowExpr(item.expr(), table, tableAlias);
            }
            boundOrderBy.add(new BoundStatement.BoundOrderBy(sortExpr, item.ascending()));
        }

        Schema outputSchema = new Schema(outputColumns);
        return new BoundStatement.BoundSelect(
                table,
                tableAlias,
                projections,
                outputSchema,
                boundWhere,
                boundOrderBy,
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
        BoundExpr bound = bindConstantOrRowExpr(whereExpr, table, tableAlias);
        DataType type = bound.dataType();
        if (type != null && type != DataType.BOOLEAN) {
            throw new BindException("WHERE clause must evaluate to BOOLEAN, got " + type);
        }
        if (type == null) {
            return new BoundExpr.BoundLiteral(Value.NULL, DataType.BOOLEAN);
        }
        return bound;
    }

    private BoundExpr bindConstantOrRowExpr(Expr expr, TableMetadata table, String tableAlias) {
        return switch (expr) {
            case Expr.LiteralExpr(Value val) -> new BoundExpr.BoundLiteral(val, val.dataType());
            case Expr.ColumnRefExpr(String qualifier, String colName) -> {
                if (table == null) {
                    throw new BindException("Column reference '" + colName + "' is not allowed in INSERT VALUES");
                }
                if (qualifier != null) {
                    validateTableQualifier(qualifier, table, tableAlias);
                }
                Schema schema = table.getSchema();
                int idx = schema.getColumnIndex(colName);
                if (idx < 0) {
                    throw new BindException("Unknown column '" + colName + "' in table '" + table.getName() + "'");
                }
                Column col = schema.getColumn(idx);
                yield new BoundExpr.BoundColumnRef(idx, col.name(), col.type());
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

                BoundExpr boundOperand = bindConstantOrRowExpr(operand, table, tableAlias);
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
                BoundExpr boundLeft = bindConstantOrRowExpr(left, table, tableAlias);
                BoundExpr boundRight = bindConstantOrRowExpr(right, table, tableAlias);
                yield bindBinaryOp(boundLeft, op, boundRight);
            }
            case Expr.IsNullExpr(Expr operand, boolean negated) -> {
                BoundExpr boundOperand = bindConstantOrRowExpr(operand, table, tableAlias);
                yield new BoundExpr.BoundIsNullExpr(boundOperand, negated);
            }
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

    private static int resolveVarcharMaxLength(BoundExpr expr, Schema tableSchema) {
        if (expr instanceof BoundExpr.BoundColumnRef(int idx, String ignoredName, DataType ignoredType)) {
            return tableSchema.getColumn(idx).maxLength();
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
