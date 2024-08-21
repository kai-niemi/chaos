package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.transaction.support.TransactionCallback;

import io.roach.chaos.model.Account;
import io.roach.chaos.model.AccountType;
import io.roach.chaos.model.LockType;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.ConsoleOutput;
import io.roach.chaos.util.Exporter;
import io.roach.chaos.util.TransactionWrapper;

import static io.roach.chaos.util.RandomData.selectRandom;

@Note("A5B write skew anomaly")
public class WriteSkewWorkload extends AbstractAccountWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private final AtomicInteger accept = new AtomicInteger();

    private final AtomicInteger reject = new AtomicInteger();

    @Override
    public List<Duration> doExecute() {
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        final List<Duration> durations = new ArrayList<>();

        TransactionCallback<Void> callback = status -> {
            Account target = selectRandom(accountSelection);

            BigDecimal amount = BigDecimal.valueOf(random.nextDouble(1, 50))
                    .setScale(2, RoundingMode.HALF_UP);

            // Invariant check using aggregate - thus can't use SFU
            BigDecimal totalBalance = accountRepository.totalAccountBalance(target.getId().getId());

            if (totalBalance.subtract(amount).compareTo(BigDecimal.ZERO) > 0) {
                accept.incrementAndGet();
                // Skew point where different threads may pick different paths
                // (allowed in snapshot and RC but not in 1SR)
                if (settings.isOptimisticLocking()) {
                    accountRepository.addBalanceCAS(
                            target.getId().getId(),
                            random.nextBoolean() ? AccountType.credit : AccountType.checking,
                            amount.negate(),
                            target.getVersion());
                } else {
                    accountRepository.addBalance(
                            target.getId().getId(),
                            random.nextBoolean() ? AccountType.credit : AccountType.checking,
                            amount.negate());
                }
            } else {
                reject.incrementAndGet();
            }

            return null;
        };

        TransactionWrapper transactionWrapper = transactionWrapper();
        transactionWrapper.execute(callback, durations::addAll);

        return durations;
    }

    @Override
    protected void preValidate() {
        if (EnumSet.of(LockType.FOR_SHARE, LockType.FOR_UPDATE).contains(settings.getLockType())) {
            ConsoleOutput.warn("This workload can't use pessimistic locks only CAS");
        }
    }

    @Override
    protected void beforeExecution() {
        this.accountSelection.addAll(accountRepository.findRandomAccounts(settings.getSelection()));
        this.accept.set(0);
        this.reject.set(0);
    }

    @Override
    protected void afterExecution(Exporter exporter) {
        ConsoleOutput.header("Consistency Check");

        ConsoleOutput.printRight("Balance updates accepted:", "%d".formatted(accept.get()));
        ConsoleOutput.printRight("Balance updates rejected:", "%d".formatted(reject.get()));

        AtomicInteger negativeAccounts = new AtomicInteger();
        AtomicReference<BigDecimal> total = new AtomicReference<>(BigDecimal.ZERO);

        accountRepository.findNegativeBalances(pair -> {
            ConsoleOutput.error("Negative balance for account tuple id: %s (%,f)"
                    .formatted(pair.getFirst(), pair.getSecond()));
            negativeAccounts.incrementAndGet();
            total.set(total.get().add(pair.getSecond()));
        });

        if (negativeAccounts.get() > 0) {
            ConsoleOutput.error("You have %d account tuples with a negative total balance! %s"
                    .formatted(negativeAccounts.get(), AsciiArt.flipTableRoughly()));
            ConsoleOutput.error("You just lost %s and may want to reconsider your isolation level!! (or use locking) %s"
                    .formatted(total.get(), AsciiArt.flipTableRoughly()));
        } else {
            ConsoleOutput.info("You are good! %s"
                    .formatted(AsciiArt.happy()));
            ConsoleOutput.info(
                    "To observe anomalies, try read-committed without locking and account narrowing (ex: --isolation rc --selection 20)");
        }

        exporter.write(List.of("discrepancies", (long) negativeAccounts.get(), "counter"));
    }
}
