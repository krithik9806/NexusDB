package io.nexusdb.core.ast;

import java.util.Objects;

/**
 * Ordering specification in an {@code ORDER BY} clause ({@code expr [ASC | DESC]}).
 */
public record OrderByItem(Expr expr, boolean ascending) {
    public OrderByItem {
        Objects.requireNonNull(expr, "expr must not be null");
    }

    public OrderByItem(Expr expr) {
        this(expr, true);
    }
}
