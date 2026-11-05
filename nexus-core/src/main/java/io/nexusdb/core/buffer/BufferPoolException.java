package io.nexusdb.core.buffer;

import io.nexusdb.core.storage.StorageException;

public class BufferPoolException extends StorageException {
    public BufferPoolException(String message) {
        super(message);
    }

    public BufferPoolException(String message, Throwable cause) {
        super(message, cause);
    }
}
