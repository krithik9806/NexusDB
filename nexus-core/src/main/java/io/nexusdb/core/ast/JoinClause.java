package io.nexusdb.core.ast;

import java.util.Objects;

/**
 * Represents a single {@code [INNER] JOIN <right> ON <condition>} or
 * {@code LEFT [OUTER] JOIN <right> ON <condition>} clause in a {@code SELECT} statement.
 */
public record JoinClause(JoinType joinType, TableRef rightTable, Expr condition) {
    public JoinClause {
        Objects.requireNonNull(joinType, "joinType must not be null");
        Objects.requireNonNull(rightTable, "rightTable must not be null");
        Objects.requireNonNull(condition, "condition must not be null");
    }
}
