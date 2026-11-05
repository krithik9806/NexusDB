package io.nexusdb.server.raft;

import io.nexusdb.core.engine.Database;
import io.nexusdb.core.engine.ResultSet;
import io.nexusdb.distributed.consensus.StateMachine;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Deterministic {@link StateMachine} applying committed Raft commands sequentially to the local {@link Database}.
 */
public final class DatabaseStateMachine implements StateMachine {
    private final Database database;

    public DatabaseStateMachine(Database database) {
        this.database = Objects.requireNonNull(database, "database must not be null");
    }

    @Override
    public synchronized byte[] apply(long index, byte[] command) {
        String sql = new String(command, StandardCharsets.UTF_8);
        try {
            ResultSet rs = database.execute(sql);
            return ("ROWS_AFFECTED:" + rs.rowsAffected()).getBytes(StandardCharsets.UTF_8);
        } catch (Throwable t) {
            return ("ERROR:" + t.getMessage()).getBytes(StandardCharsets.UTF_8);
        }
    }
}
