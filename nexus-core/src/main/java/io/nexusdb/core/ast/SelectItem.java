package io.nexusdb.core.ast;

import java.util.Objects;

/**
 * Projection item in a {@code SELECT} list: either {@code *} / {@code table.*}
 * or an expression with an optional alias.
 */
public sealed interface SelectItem
        permits SelectItem.AllColumns,
                SelectItem.ExprItem {

    /**
     * Represents {@code *} (when {@code tableQualifier == null}) or {@code table.*}
     * (when {@code tableQualifier != null}).
     */
    record AllColumns(String tableQualifier) implements SelectItem {
        public AllColumns() {
            this(null);
        }
    }

    /**
     * Represents {@code expr [AS alias]}.
     */
    record ExprItem(Expr expr, String alias) implements SelectItem {
        public ExprItem {
            Objects.requireNonNull(expr, "expr must not be null");
        }

        public ExprItem(Expr expr) {
            this(expr, null);
        }
    }
}
