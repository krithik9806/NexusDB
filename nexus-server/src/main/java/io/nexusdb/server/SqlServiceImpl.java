package io.nexusdb.server;

import io.grpc.stub.StreamObserver;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.proto.SqlRequest;
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.proto.SqlRow;
import io.nexusdb.proto.SqlServiceGrpc;

import java.util.Objects;

/**
 * gRPC service handling remote SQL query and DDL/DML execution against a {@link Database} instance.
 */
public final class SqlServiceImpl extends SqlServiceGrpc.SqlServiceImplBase {
    private final Database database;

    public SqlServiceImpl(Database database) {
        this.database = Objects.requireNonNull(database, "database must not be null");
    }

    @Override
    public void execute(SqlRequest request, StreamObserver<SqlResponse> responseObserver) {
        String sql = request.getSql();
        try {
            ResultSet rs = database.execute(sql);
            SqlResponse.Builder builder = SqlResponse.newBuilder()
                    .setSuccess(true)
                    .setRowsAffected(rs.rowsAffected())
                    .setExecutionTimeMs(rs.executionTimeMs());

            for (Column col : rs.columns()) {
                builder.addColumns(col.name());
            }

            for (Tuple tuple : rs.rows()) {
                SqlRow.Builder rowBuilder = SqlRow.newBuilder();
                for (int i = 0; i < rs.columns().size(); i++) {
                    Value val = tuple.getValue(i);
                    rowBuilder.addValues(formatValue(val));
                }
                builder.addRows(rowBuilder.build());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            SqlResponse response = SqlResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static String formatValue(Value v) {
        if (v == null || v instanceof Value.NullValue) {
            return "NULL";
        }
        if (v instanceof Value.IntValue iv) {
            return String.valueOf(iv.value());
        }
        if (v instanceof Value.BigIntValue bv) {
            return String.valueOf(bv.value());
        }
        if (v instanceof Value.DoubleValue dv) {
            return String.valueOf(dv.value());
        }
        if (v instanceof Value.BooleanValue bv) {
            return String.valueOf(bv.value());
        }
        if (v instanceof Value.VarcharValue vv) {
            return vv.value();
        }
        return v.toString();
    }
}
