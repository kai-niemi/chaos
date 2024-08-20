package io.roach.chaos.util;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;

public class RetryableTransactionWrapper extends TransactionWrapper {
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private static final int MAX_RETRIES = 30;

    private boolean retryJitter;

    private int maxRetries = MAX_RETRIES;

    public RetryableTransactionWrapper(PlatformTransactionManager transactionManager) {
        super(transactionManager);
    }

    public RetryableTransactionWrapper setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public RetryableTransactionWrapper setRetryJitter(boolean retryJitter) {
        this.retryJitter = retryJitter;
        return this;
    }

    @Override
    public <T> T execute(TransactionCallback<T> action,
                         Consumer<List<Duration>> transactionTimes) {
        final DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        transactionDefinition.setName(Thread.currentThread().getName());
        transactionDefinition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transactionDefinition.setIsolationLevelName("ISOLATION_" + isolationLevel.name());

        final List<Duration> times = new ArrayList<>(maxRetries);

        for (int iteration = 1; iteration <= maxRetries; iteration++) {
            final Instant startTime = Instant.now();

            final TransactionStatus status = transactionManager.getTransaction(transactionDefinition);

            try {
                T result = action.doInTransaction(status);

                transactionManager.commit(status);

                times.add(Duration.between(startTime, Instant.now()));
                transactionTimes.accept(times);

                if (iteration > 1) {
                    final DoubleSummaryStatistics summary = times
                            .stream()
                            .mapToDouble(Duration::toMillis)
                            .sorted()
                            .summaryStatistics();
                    logger.debug("Recovered from transient error in call %d/%d (total: %.0f ms, avg: %.0f ms)"
                            .formatted(
                                    iteration,
                                    maxRetries,
                                    summary.getSum(),
                                    summary.getAverage()
                            ));
                }

                return result;
            } catch (TransactionSystemException ex) {
                // retry but skip rollback on commit errors
                handleTransientException(ex, iteration);
            } catch (TransientDataAccessException ex) {
                rollbackOnException(status, ex);
                handleTransientException(ex, iteration);
            } catch (Exception ex) {
                rollbackOnException(status, ex);

                // Extract SQL exception state
                Throwable cause = NestedExceptionUtils.getMostSpecificCause(ex);
                if (cause instanceof SQLException sqlException) {
                    if ("40001".equals(sqlException.getSQLState()) ||
                            "40P01".equals(sqlException.getSQLState())) { // deadlock loser
                        handleTransientException(sqlException, iteration);
                    } else {
                        throw ex;
                    }
                } else {
                    throw ex;
                }
            } catch (Throwable ex) {
                // Fatal error
                rollbackOnException(status, ex);
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }

            times.add(Duration.between(startTime, Instant.now()));
        }

        throw new ConcurrencyFailureException("Too many transient errors %d - giving up".formatted(maxRetries));
    }

    private void handleTransientException(Exception exception,
                                          int numCalls) {
        try {
            long backoffMillis = backoffMillis(numCalls);
            if (logger.isWarnEnabled()) {
                if (exception instanceof SQLException) {
                    logger.warn("Transient SQL error (%s) in call %d/%d (backoff for %d ms): %s"
                            .formatted(((SQLException) exception).getSQLState(),
                                    numCalls,
                                    maxRetries,
                                    backoffMillis,
                                    exception.getMessage()));
                } else {
                    logger.warn("Transient error in call %d/%d (backoff for %d ms): %s"
                            .formatted(
                                    numCalls,
                                    maxRetries,
                                    backoffMillis,
                                    exception.toString()));
                }
            }
            TimeUnit.MILLISECONDS.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private long backoffMillis(int numCalls) {
        long jitter = retryJitter ? ThreadLocalRandom.current().nextInt(1000) : 0;
        double expBackoff = Math.pow(2.0, numCalls) + 100;
        return Math.min((long) (expBackoff + jitter), MAX_BACKOFF.toMillis());
    }
}
