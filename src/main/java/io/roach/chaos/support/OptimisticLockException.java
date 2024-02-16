package io.roach.chaos.support;

public class OptimisticLockException extends DataAccessException {
    public OptimisticLockException(String message) {
        super(message);
    }
}
