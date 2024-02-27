package io.roach.chaos;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import io.roach.chaos.support.AsciiArt;
import io.roach.chaos.support.JdbcUtils;
import io.roach.chaos.support.TransactionTemplate;
import io.roach.chaos.support.Tuple;

import static io.roach.chaos.AccountRepository.findById;
import static io.roach.chaos.AccountRepository.findRandomAccounts;
import static io.roach.chaos.AccountRepository.updateBalance;
import static io.roach.chaos.AccountRepository.updateBalanceCAS;
import static io.roach.chaos.support.RandomData.selectRandom;

public class LostUpdate extends AbstractWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private BigDecimal initialBalance;

    @Override
    public void beforeExecution(Output output) throws Exception {
        if (!settings.skipCreate) {
            output.info("Creating schema");
            AccountRepository.createSchema(dataSource);

            output.info("Creating %,d accounts".formatted(settings.numAccounts));
            AccountRepository.createAccounts(dataSource,
                    new BigDecimal("5000.00"), settings.numAccounts);
        }

        this.initialBalance = JdbcUtils.execute(dataSource, AccountRepository::sumTotalBalance);

        this.accountSelection.addAll(JdbcUtils.execute(dataSource,
                conn -> findRandomAccounts(conn, settings.selection)));
    }

    @Override
    public List<Duration> call() {
        List<Tuple<Account, BigDecimal>> legs = new ArrayList<>();

        Set<Account.Id> consumedIds = new HashSet<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();

        IntStream.rangeClosed(1, Integer.MAX_VALUE)
                .takeWhile(v -> legs.size() != settings.level)
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

        new TransactionTemplate(dataSource, settings.jitter)
                .executeWithRetries(conn -> {
                    BigDecimal checksum = BigDecimal.ZERO;

                    for (Tuple<Account, BigDecimal> leg : legs) {
                        Account account = findById(conn, leg.getA().getId(), settings.lock);

                        if (settings.cas) {
                            updateBalanceCAS(conn, account.addBalance(leg.getB()));
                        } else {
                            updateBalance(conn, account.addBalance(leg.getB()));
                        }

                        checksum = checksum.add(leg.getB());
                    }

                    if (checksum.compareTo(BigDecimal.ZERO) != 0) {
                        throw new IllegalStateException(
                                "Sum of account legs must equal 0 (got " + checksum.toPlainString() + ")"
                        );
                    }
                    return null;
                }, durations::addAll);

        return durations;
    }

    @Override
    public void afterExecution(Output output) {
        BigDecimal finalBalance = JdbcUtils.execute(dataSource, AccountRepository::sumTotalBalance);

        output.pair("Initial total balance:", "%s".formatted(initialBalance));
        output.pair("Final total balance:", "%s".formatted(finalBalance));

        if (!initialBalance.equals(finalBalance)) {
            output.error("%s != %s %s"
                    .formatted(initialBalance, finalBalance, AsciiArt.flipTableRoughly()));
            output.error("You just lost %s and may want to reconsider your isolation level!! (or use --sfu or --cas)"
                    .formatted(initialBalance.subtract(finalBalance)));
        } else {
            output.info("You are good! %s".formatted(AsciiArt.shrug()));
        }
    }
}
