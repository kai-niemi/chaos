package io.roach.chaos.jdbc;

public class OptimisticLockException extends DataAccessException {
    public OptimisticLockException(String message) {
        super(message);
    }
}
