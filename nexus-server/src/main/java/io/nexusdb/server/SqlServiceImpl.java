package io.nexusdb.server;

import io.grpc.stub.StreamObserver;
import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.core.record.Column;
import io.nexusdb.core.record.Tuple;
import io.nexusdb.core.record.Value;
import io.nexusdb.distributed.consensus.RaftNode;
import io.nexusdb.distributed.consensus.RaftRole;
import io.nexusdb.proto.SqlRequest;
import io.nexusdb.proto.SqlResponse;
import io.nexusdb.proto.SqlRow;
import io.nexusdb.proto.SqlServiceGrpc;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * gRPC service handling remote SQL query and DDL/DML execution.
 * In replicated mode, write statements are proposed through Raft consensus and committed across quorum,
 * while read queries are served from the local database.
 */
public final class SqlServiceImpl extends SqlServiceGrpc.SqlServiceImplBase {
    private final Database database;
    private final RaftNode raftNode;

    public SqlServiceImpl(Database database) {
        this(database, null);
    }

    public SqlServiceImpl(Database database, RaftNode raftNode) {
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.raftNode = raftNode;
    }

    @Override
    public void execute(SqlRequest request, StreamObserver<SqlResponse> responseObserver) {
        String sql = request.getSql().trim();
        try {
            boolean isWriteStatement = isWrite(sql);

            if (raftNode != null && isWriteStatement) {
                if (raftNode.getRole() != RaftRole.LEADER) {
                    String leader = raftNode.getLeaderId();
                    SqlResponse redirect = SqlResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage("NOT_LEADER: leader is " + (leader != null ? leader : "unknown"))
                            .build();
                    responseObserver.onNext(redirect);
                    responseObserver.onCompleted();
                    return;
                }

                // Propose write to Raft consensus group
                byte[] command = sql.getBytes(StandardCharsets.UTF_8);
                byte[] result = raftNode.propose(command).get(15, TimeUnit.SECONDS);
                String resStr = new String(result, StandardCharsets.UTF_8);

                if (resStr.startsWith("ERROR:")) {
                    SqlResponse err = SqlResponse.newBuilder()
                            .setSuccess(false)
                            .setErrorMessage(resStr.substring("ERROR:".length()))
                            .build();
                    responseObserver.onNext(err);
                    responseObserver.onCompleted();
                    return;
                }

                int rowsAffected = 0;
                if (resStr.startsWith("ROWS_AFFECTED:")) {
                    try {
                        rowsAffected = Integer.parseInt(resStr.substring("ROWS_AFFECTED:".length()));
                    } catch (NumberFormatException ignored) {
                    }
                }

                SqlResponse success = SqlResponse.newBuilder()
                        .setSuccess(true)
                        .setRowsAffected(rowsAffected)
                        .build();
                responseObserver.onNext(success);
                responseObserver.onCompleted();
                return;
            }

            // Local execution for read queries or standalone mode
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

    private static boolean isWrite(String sql) {
        String upper = sql.trim().toUpperCase(Locale.ROOT);
        return upper.startsWith("INSERT")
                || upper.startsWith("UPDATE")
                || upper.startsWith("DELETE")
                || upper.startsWith("CREATE")
                || upper.startsWith("DROP")
                || upper.startsWith("BEGIN")
                || upper.startsWith("COMMIT")
                || upper.startsWith("ROLLBACK");
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
