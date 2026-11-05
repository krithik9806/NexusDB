package io.nexusdb.core.index;

/**
 * Thrown when attempting to insert a duplicate key with a different {@code RecordId}
 * into a unique {@link BPlusTree} index.
 */
public final class DuplicateKeyException extends RuntimeException {
    public DuplicateKeyException(String message) {
        super(message);
    }

    public DuplicateKeyException(String message, Throwable cause) {
        super(message, cause);
    }
}
