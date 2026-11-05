package io.nexusdb.distributed.sharding;

public class UnsupportedCrossShardOperationException extends RuntimeException {
    public UnsupportedCrossShardOperationException(String message) {
        super(message);
    }
}
