package io.roach.chaos.support;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TransactionTemplate {
    private TransactionTemplate() {
    }

    private static final Logger logger = LoggerFactory.getLogger(TransactionTemplate.class);

    public static <T> T execute(DataSource ds,
                                TransactionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            T result;
            try {
                result = action.doInTransaction(conn);
            } catch (RuntimeException | Error ex) {
                conn.rollback();
                throw ex;
            } catch (Throwable ex) {
                conn.rollback();
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
            conn.commit();
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    public static <T> T executeWithRetries(DataSource ds,
                                           TransactionCallback<T> action,
                                           Consumer<List<Duration>> transactionTimes) {
        int maxCalls = 15;

        final List<Duration> times = new ArrayList<>(maxCalls);

        for (int call = 1; call <= maxCalls; call++) {
            final Instant startTime = Instant.now();

            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                try {
                    T result = action.doInTransaction(conn);

                    conn.commit();

                    times.add(Duration.between(startTime, Instant.now()));
                    transactionTimes.accept(times);

                    return result;
                } catch (SQLException ex) {
                    if ("40001".equals(ex.getSQLState())) {
                        conn.rollback();
                        handleTransientException(ex, call, maxCalls);
                        times.add(Duration.between(startTime, Instant.now()));
                    } else {
                        throw ex;
                    }
                } catch (OptimisticLockException ex) {
                    conn.rollback();
                    handleTransientException(ex, call, maxCalls);
                    times.add(Duration.between(startTime, Instant.now()));
                } catch (Throwable ex) {
                    conn.rollback();
                    throw new UndeclaredThrowableException(ex,
                            "TransactionCallback threw undeclared checked exception");
                }
            } catch (SQLException e) {
                throw new DataAccessException("Failed to connect to database", e);
            }
        }

        throw new DataAccessException("Too many transient errors %d - giving up".formatted(maxCalls));
    }

    public static final Duration MAX_BACKOFF = Duration.ofSeconds(15);

    private static void handleTransientException(SQLException sqlException, int numCalls, int maxCalls) {
        try {
            // skip the jitter for more comparable results between isolation levels
            long backoffMillis = Math.min((long) (Math.pow(2, numCalls)), MAX_BACKOFF.toMillis());
            if (numCalls <= 1 && logger.isWarnEnabled()) {
                logger.warn("Transient SQL error (%s) in call %d/%d (backoff for %d ms before retry): %s"
                        .formatted(sqlException.getSQLState(),
                                numCalls,
                                maxCalls,
                                backoffMillis,
                                sqlException.getMessage()));
            }
            TimeUnit.MILLISECONDS.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void handleTransientException(OptimisticLockException optimisticLockException,
                                                 int numCalls, int maxCalls) {
        try {
            // skip the jitter for more comparable results between isolation levels
            long backoffMillis = Math.min((long) (Math.pow(2, numCalls)), MAX_BACKOFF.toMillis());
            if (numCalls <= 1 && logger.isWarnEnabled()) {
                logger.warn("Optimistic lock exception in call %d/%d (backoff for %d ms before retry): %s"
                        .formatted(
                                numCalls,
                                maxCalls,
                                backoffMillis,
                                optimisticLockException.getMessage()));
            }
            TimeUnit.MILLISECONDS.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
