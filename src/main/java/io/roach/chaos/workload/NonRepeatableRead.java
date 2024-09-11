package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.springframework.transaction.support.TransactionCallback;

import io.roach.chaos.model.Account;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.TransactionWrapper;

@Note("P2 non-repeatable / fuzzy read anomaly")
public class NonRepeatableRead extends AbstractWorkload {
    private Collection<Account> accountSelection = List.of();

    private final int repeatedReads = 10;

    private final Map<Account.Id, Set<BigDecimal>> anomalies = Collections.synchronizedMap(new HashMap<>());

    private final AtomicInteger reads = new AtomicInteger();

    private final AtomicInteger writes = new AtomicInteger();

    @Override
    protected void doBeforeExecutions() {
        this.accountSelection = accountRepository.findTargetAccounts(settings.getSelection(), settings.isRandomSelection());
    }

    @Override
    public List<Duration> oneExecution() {
        // Let's roll with 10% writes
        if (ThreadLocalRandom.current().nextDouble(1.00) < settings.getReadWriteRatio()) {
            reads.incrementAndGet();
            return readRows();
        }
        writes.incrementAndGet();
        return writeRows();
    }

    private List<Duration> readRows() {
        final Map<Account.Id, List<BigDecimal>> balanceObservations = new LinkedHashMap<>();

        // Within the same transaction, all reads must return the same value otherwise its a P2 anomaly
        TransactionCallback<Void> callback = status -> {
            // Clear previous observation on retries
            balanceObservations.clear();

            accountSelection.forEach(a -> {
                // Add write mutex scoped by account id
                IntStream.rangeClosed(1, repeatedReads)
                        .forEach(value -> {
                            Account account = accountRepository.findAccountById(a.getId(), settings.getLockType());

                            balanceObservations.computeIfAbsent(a.getId(),
                                            x -> new ArrayList<>())
                                    .add(account.getBalance());
                        });
            });
            return null;
        };

        final List<Duration> durations = new ArrayList<>();

        TransactionWrapper transactionWrapper = transactionWrapper();
        transactionWrapper.execute(callback, durations::addAll);

        // Sum up for reporting
        balanceObservations.forEach((id, balances) -> {
            List<BigDecimal> distinctValues = balances.stream().distinct().toList();
            if (distinctValues.size() != 1) {
                anomalies.computeIfAbsent(id, x -> new TreeSet<>())
                        .addAll(distinctValues);
            }
        });

        return durations;
    }

    private List<Duration> writeRows() {
        TransactionCallback<Void> callback = status -> {
            accountSelection.forEach(a -> {
                if (settings.isOptimisticLocking()) {
                    accountRepository.updateBalanceCAS(a.addBalance(BigDecimal.ONE));
                } else {
                    accountRepository.updateBalance(a.addBalance(BigDecimal.ONE));
                }
            });
            return null;
        };

        final List<Duration> durations = new ArrayList<>();

        TransactionWrapper transactionWrapper = transactionWrapper();
        transactionWrapper.execute(callback, durations::addAll);

        return durations;
    }

    @Override
    public void afterAllExecutions() {
        logger.highlight("Consistency Check");

        anomalies.forEach((id, balances) ->
                logger.error("Observed non-repeatable values for key %s: %s".formatted(id, balances)));

        logger.info("Total reads: %d".formatted(reads.get()));
        logger.info("Total writes: %d".formatted(writes.get()));

        if (anomalies.isEmpty()) {
            logger.info("You are good! %s".formatted(AsciiArt.happy()));
            logger.info("To observe anomalies, try read-committed without locking (--isolation rc)");
        } else {
            logger.error("Observed %d accounts returning non-repeatable reads! %s"
                    .formatted(anomalies.size(), AsciiArt.flipTableRoughly()));
            logger.info(
                    "To avoid anomalies, try read-committed with locking or repeatable-read or higher isolation (--locking for_share)");
        }
    }
}

