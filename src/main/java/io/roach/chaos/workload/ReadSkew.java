package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.transaction.support.TransactionCallback;

import io.roach.chaos.model.Account;
import io.roach.chaos.model.AccountType;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.TransactionWrapper;
import io.roach.chaos.util.Tuple;

import static io.roach.chaos.util.RandomData.selectRandom;

@Note("A5A read skew anomaly")
public class ReadSkew extends AbstractWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private final List<Tuple<Long, BigDecimal>> discrepancies = Collections.synchronizedList(new ArrayList<>());

    private final BlockingQueue<Tuple<Account.Id, Account.Id>> queue = new LinkedBlockingQueue<>(100);

    private final BigDecimal tupleSum = new BigDecimal("1000.00");

    @Override
    public List<Duration> oneExecution() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        final List<Duration> durations = new ArrayList<>();

        TransactionCallback<Void> callback = status -> {
            // Drain queue and read each account in tuple separately (rather than using aggregation)
            Tuple<Account.Id, Account.Id> tuple = queue.poll();
            while (tuple != null) {
                Account a = accountRepository.findAccountById(tuple.getA(), settings.getLockType());
                Account b = accountRepository.findAccountById(tuple.getB(), settings.getLockType());

                BigDecimal snapshot = a.getBalance().add(b.getBalance());

                // Should always observe a constant total
                if (!snapshot.equals(tupleSum)) {
                    discrepancies.add(Tuple.of(a.getId().getId(), snapshot));
                }

                tuple = queue.poll();
            }

            Account account = selectRandom(accountSelection);

            BigDecimal totalBalance = accountRepository.totalAccountBalance(account.getId().getId());

            BigDecimal amount = BigDecimal.valueOf(random.nextDouble(10, 150))
                    .setScale(2, RoundingMode.HALF_UP);

            if (totalBalance.subtract(amount).compareTo(BigDecimal.ZERO) >= 0) {
                accountRepository.addBalance(account.getId().getId(), AccountType.credit,
                        amount.negate());
                accountRepository.addBalance(account.getId().getId(), AccountType.checking,
                        amount);

                try {
                    queue.put(Tuple.of(
                            new Account.Id(account.getId().getId(), AccountType.credit),
                            new Account.Id(account.getId().getId(), AccountType.checking))
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

            return null;
        };

        TransactionWrapper transactionWrapper = transactionWrapper();
        transactionWrapper.execute(callback, durations::addAll);

        return durations;
    }

    @Override
    protected void doBeforeExecutions() {
        this.accountSelection.addAll(
                accountRepository.findTargetAccounts(settings.getSelection(), settings.isRandomSelection()));
        this.discrepancies.clear();
    }

    @Override
    public void afterAllExecutions() {
        logger.highlight("Consistency Check");

        AtomicInteger negativeAccounts = new AtomicInteger();
        AtomicReference<BigDecimal> total = new AtomicReference<>(BigDecimal.ZERO);

        accountRepository.findNegativeBalances(pair -> {
            logger.error("Negative balance for account tuple id: %s (%,f)"
                    .formatted(pair.getFirst(), pair.getSecond()));
            negativeAccounts.incrementAndGet();
            total.set(total.get().add(pair.getSecond()));
        });

        if (discrepancies.isEmpty()) {
            logger.info("No account balance discrepancies %s"
                    .formatted(AsciiArt.happy()));
        } else {
            discrepancies
                    .stream()
                    .limit(10)
                    .forEach(tuple -> logger.error(
                            "Observed inconsistent sum for account tuple id: %s (%,.2f) - expected %,.2f"
                                    .formatted(tuple.getA(), tuple.getB(), tupleSum)));
            logger.error("Total of %d account balance discrepancies (listing top 10): %s"
                    .formatted(discrepancies.size(), AsciiArt.flipTableRoughly()));
        }

        if (negativeAccounts.get() > 0) {
            logger.error("You have %d account tuples with a negative total balance! %s"
                    .formatted(negativeAccounts.get(), AsciiArt.flipTableRoughly()));
            logger.error("You just lost %s and may want to reconsider your isolation level!! (or use locking)"
                    .formatted(total.get()));
        } else {
            logger.info("No negative balances %s"
                    .formatted(AsciiArt.happy()));

            if (discrepancies.isEmpty()) {
                logger.info("To observe anomalies, try read-committed without locking and account narrowing "
                        + "(--isolation rc --selection 20)");
            }
        }
    }
}
