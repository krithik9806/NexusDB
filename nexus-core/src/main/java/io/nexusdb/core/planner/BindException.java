package io.nexusdb.core.planner;

/**
 * Thrown by {@link Binder} when a parsed SQL statement fails catalog resolution,
 * type checking, arity verification, or constraint validation.
 */
public final class BindException extends RuntimeException {
    public BindException(String message) {
        super(message);
    }

    public BindException(String message, Throwable cause) {
        super(message, cause);
    }
}
