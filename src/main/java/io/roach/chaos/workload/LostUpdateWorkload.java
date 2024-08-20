package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.springframework.transaction.support.TransactionCallback;

import io.roach.chaos.model.Account;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.ConsoleOutput;
import io.roach.chaos.util.Exporter;
import io.roach.chaos.util.TransactionWrapper;
import io.roach.chaos.util.Tuple;

import static io.roach.chaos.util.RandomData.selectRandom;

@Note("P4 lost update anomaly")
public class LostUpdateWorkload extends AbstractAccountWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private BigDecimal initialBalance;

    @Override
    public List<Duration> doExecute() {
        final List<Tuple<Account, BigDecimal>> legs = new ArrayList<>();
        final Set<Account.Id> consumedIds = new HashSet<>();
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        IntStream.rangeClosed(1, Integer.MAX_VALUE)
                .takeWhile(v -> legs.size() != settings.getContentionLevel())
                .forEach(leg -> {
                    Account from = selectRandom(accountSelection);
                    Account to = selectRandom(accountSelection);

                    if (consumedIds.add(from.getId()) && consumedIds.add(to.getId())) {
                        BigDecimal amount = BigDecimal.valueOf(random.nextDouble(1, 10))
                                .setScale(2, RoundingMode.HALF_UP);

                        legs.add(Tuple.of(from, amount.negate()));
                        legs.add(Tuple.of(to, amount));
                    }
                });

        List<Duration> durations = new ArrayList<>();

        TransactionCallback<Void> callback = status -> {
            BigDecimal checksum = BigDecimal.ZERO;

            for (Tuple<Account, BigDecimal> leg : legs) {
                Account account = accountRepository.findById(leg.getA().getId(), settings.getLockType());

                if (settings.isOptimisticLocking()) {
                    accountRepository.updateBalanceCAS(account.addBalance(leg.getB()));
                } else {
                    accountRepository.updateBalance(account.addBalance(leg.getB()));
                }

                checksum = checksum.add(leg.getB());
            }

            if (checksum.compareTo(BigDecimal.ZERO) != 0) {
                throw new IllegalStateException(
                        "Sum of account legs must equal 0 (got " + checksum.toPlainString() + ")"
                );
            }

            return null;
        };

        TransactionWrapper transactionWrapper = transactionWrapper();
        transactionWrapper.execute(callback, durations::addAll);

        return durations;
    }

    @Override
    protected void beforeExecution() {
        this.initialBalance = accountRepository.sumTotalBalance();
        this.accountSelection.addAll(accountRepository.findRandomAccounts(settings.getSelection()));
    }

    @Override
    protected void afterExecution(Exporter exporter) {
        ConsoleOutput.header("Consistency Check");

        BigDecimal finalBalance = accountRepository.sumTotalBalance();

        ConsoleOutput.printRight("Initial total balance:", "%s".formatted(initialBalance));
        ConsoleOutput.printRight("Final total balance:", "%s".formatted(finalBalance));

        if (!initialBalance.equals(finalBalance)) {
            ConsoleOutput.error("%s != %s %s"
                    .formatted(initialBalance, finalBalance, AsciiArt.flipTableRoughly()));
            ConsoleOutput.error("You just lost %s and may want to reconsider your isolation level!! (or use locking)"
                    .formatted(initialBalance.subtract(finalBalance)));
        } else {
            ConsoleOutput.info("You are good! %s (try weaker isolation using --isolation)".formatted(AsciiArt.shrug()));
        }

        exporter.write(List.of("discrepancies", initialBalance.equals(finalBalance) ? 0 : 1, "counter"));
    }
}
