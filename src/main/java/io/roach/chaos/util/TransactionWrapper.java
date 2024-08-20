package io.roach.chaos.util;

import java.lang.reflect.UndeclaredThrowableException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;

import io.roach.chaos.model.IsolationLevel;

public class TransactionWrapper {
    protected final Logger logger = LoggerFactory.getLogger(TransactionWrapper.class);

    protected final PlatformTransactionManager transactionManager;

    protected IsolationLevel isolationLevel = IsolationLevel.SERIALIZABLE;

    public TransactionWrapper(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public TransactionWrapper setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        return this;
    }

    public <T> T execute(TransactionCallback<T> action,
                         Consumer<List<Duration>> transactionTimes) {
        final DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
        transactionDefinition.setName(Thread.currentThread().getName());
        transactionDefinition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transactionDefinition.setIsolationLevelName("ISOLATION_" + isolationLevel.name());

        final Instant startTime = Instant.now();

        final TransactionStatus status = transactionManager.getTransaction(transactionDefinition);

        try {
            T result = action.doInTransaction(status);

            transactionManager.commit(status);

            final List<Duration> times = new ArrayList<>();
            times.add(Duration.between(startTime, Instant.now()));
            transactionTimes.accept(times);

            return result;
        } catch (DataAccessException ex) {
            rollbackOnException(status, ex);
            throw ex;
        } catch (Throwable ex) {
            rollbackOnException(status, ex);
            throw new UndeclaredThrowableException(ex,
                    "TransactionCallback threw undeclared checked exception");
        }
    }

    protected void rollbackOnException(TransactionStatus status, Throwable ex) throws TransactionException {
        try {
            if (!status.isCompleted()) {
                logger.debug("Initiating transaction rollback on application exception", ex);
                this.transactionManager.rollback(status);
            }
        } catch (TransactionSystemException ex2) {
            logger.error("Application exception overridden by rollback exception", ex);
            ex2.initApplicationException(ex);
            throw ex2;
        } catch (RuntimeException ex2) {
            logger.error("Application exception overridden by rollback exception", ex);
            throw ex2;
        } catch (Error err) {
            logger.error("Application exception overridden by rollback error", ex);
            throw err;
        }
    }
}
