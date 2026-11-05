package io.nexusdb.core.ast;

/**
 * Join types supported in the NexusDB SQL dialect.
 */
public enum JoinType {
    INNER("INNER JOIN"),
    LEFT("LEFT JOIN");

    private final String sql;

    JoinType(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }
}
