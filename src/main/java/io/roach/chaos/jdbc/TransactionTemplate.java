package io.roach.chaos.jdbc;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionTemplate {
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(15);

    private static final int MAX_RETRIES = 15;

    private final Logger logger = LoggerFactory.getLogger(TransactionTemplate.class);

    private final DataSource ds;

    private final boolean retryJitter;

    private final boolean retries;

    public TransactionTemplate(DataSource ds, boolean retryJitter, boolean retries) {
        this.ds = ds;
        this.retryJitter = retryJitter;
        this.retries = retries;
    }

    public <T> T execute(TransactionCallback<T> action,
                         Consumer<List<Duration>> transactionTimes) {
        return retries
                ? executeWithRetries(action, transactionTimes)
                : executeWithoutRetries(action, transactionTimes);
    }

    public <T> T executeWithRetries(TransactionCallback<T> action,
                                    Consumer<List<Duration>> transactionTimes) {
        final List<Duration> times = new ArrayList<>(MAX_RETRIES);

        for (int call = 1; call <= MAX_RETRIES; call++) {
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
                    if ("40001".equals(ex.getSQLState()) ||
                            "40P01".equals(ex.getSQLState())) { // deadlock loser
                        conn.rollback();
                        handleTransientException(ex, call, MAX_RETRIES);
                        times.add(Duration.between(startTime, Instant.now()));
                    } else {
                        throw ex;
                    }
                } catch (OptimisticLockException ex) {
                    conn.rollback();
                    handleTransientException(ex, call, MAX_RETRIES);
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

        throw new DataAccessException("Too many transient errors %d - giving up".formatted(MAX_RETRIES));
    }

    public <T> T executeWithoutRetries(TransactionCallback<T> action,
                                       Consumer<List<Duration>> transactionTimes) {
        final List<Duration> times = new ArrayList<>();

        final Instant startTime = Instant.now();

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                T result = action.doInTransaction(conn);
                conn.commit();
                times.add(Duration.between(startTime, Instant.now()));
                transactionTimes.accept(times);
                return result;
            } catch (SQLException e) {
                conn.rollback();
                throw new DataAccessException(e);
            } catch (Throwable ex) {
                conn.rollback();
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }

    private long backoffMillis(int numCalls) {
        long jitter = retryJitter ? ThreadLocalRandom.current().nextInt(1000) : 0;
        double expBackoff = Math.pow(2.0, numCalls) + 100;
        return Math.min((long) (expBackoff + jitter), MAX_BACKOFF.toMillis());
    }

    private void handleTransientException(SQLException sqlException, int numCalls, int maxCalls) {
        try {
            long backoffMillis = backoffMillis(numCalls);
            if (logger.isWarnEnabled()) {
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

    private void handleTransientException(OptimisticLockException optimisticLockException,
                                          int numCalls, int maxCalls) {
        try {
            long backoffMillis = backoffMillis(numCalls);
            if (logger.isWarnEnabled()) {
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
