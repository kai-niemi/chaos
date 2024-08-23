package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.transaction.support.TransactionCallback;

import io.roach.chaos.model.Account;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.ConsoleOutput;
import io.roach.chaos.util.TransactionWrapper;
import io.roach.chaos.util.Tuple;

import static io.roach.chaos.util.RandomData.selectRandomUnique;

@Note("P4 lost update anomaly")
public class LostUpdate extends AbstractWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private BigDecimal initialBalance;

    @Override
    public void validateSettings() {
        if (settings.getSelection() <= settings.getContentionLevel()) {
            throw new IllegalStateException("Account selection must be > than contention level");
        }
    }

    @Override
    public List<Duration> oneExecution() {
        final Collection<Account> accounts = selectRandomUnique(accountSelection, settings.getContentionLevel());

        final BigDecimal amount = BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(1, 10))
                .setScale(2, RoundingMode.HALF_UP);

        final List<Tuple<Account, BigDecimal>> legs = new ArrayList<>();

        for (Account account : accounts) {
            if (legs.size() % 2 == 0) {
                legs.add(Tuple.of(account, amount));
            } else {
                legs.add(Tuple.of(account, amount.negate()));
            }
        }

        TransactionCallback<Void> callback = status -> {
            BigDecimal checksum = BigDecimal.ZERO;

            for (Tuple<Account, BigDecimal> leg : legs) {
                Account account = accountRepository.findAccountById(leg.getA().getId(), settings.getLockType());

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

        final List<Duration> durations = new ArrayList<>();

        TransactionWrapper transactionWrapper = transactionWrapper();
        transactionWrapper.execute(callback, durations::addAll);

        return durations;
    }

    @Override
    protected void doBeforeExecutions() {
        this.initialBalance = accountRepository.sumTotalBalance();
        this.accountSelection.addAll(accountRepository.findTargetAccounts(settings.getSelection(), settings.isRandomSelection()));
    }

    @Override
    public void afterAllExecutions() {
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
            ConsoleOutput.info("You are good! %s".formatted(AsciiArt.happy()));
            ConsoleOutput.info("To observe anomalies, try read-committed without locking (ex: --isolation rc)");
        }
    }
}
