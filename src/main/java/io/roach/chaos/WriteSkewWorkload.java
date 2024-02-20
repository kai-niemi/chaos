package io.roach.chaos;

import io.roach.chaos.support.JdbcUtils;
import io.roach.chaos.support.TransactionTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.roach.chaos.AccountRepository.findRandomAccounts;
import static io.roach.chaos.support.RandomData.selectRandom;

public class WriteSkewWorkload extends AbstractWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    @Override
    public void beforeExecution(Output output) throws Exception {
        if (!settings.skipCreate) {
            output.info("Creating schema");
            AccountRepository.createSchema(dataSource);

            output.info("Creating %,d accounts".formatted(settings.numAccounts));
            AccountRepository.createAccounts(dataSource,
                    new BigDecimal("500.00"), settings.numAccounts);
        }

        this.accountSelection.addAll(JdbcUtils.execute(dataSource,
                conn -> findRandomAccounts(conn, settings.selection)));
    }

    @Override
    public List<Duration> call() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        List<Duration> durations = new ArrayList<>();

        new TransactionTemplate(dataSource, settings.jitter)
                .executeWithRetries(conn -> {
                    Account target = selectRandom(accountSelection);

                    BigDecimal amount = BigDecimal.valueOf(random.nextDouble(1, 50))
                            .setScale(2, RoundingMode.HALF_UP);

                    // Invariant check - can't use SFU
                    BigDecimal totalBalance = AccountRepository.readTotalBalance(conn, target.getId());
                    if (totalBalance.subtract(amount).compareTo(BigDecimal.ZERO) > 0) {
                        // Skew point where different threads may pick different paths
                        // (allowed in snapshot and RC but not in 1SR)
                        if (settings.cas) {
                            AccountRepository.updateBalanceCAS(conn,
                                    target,
                                    random.nextBoolean() ? AccountType.credit : AccountType.checking,
                                    amount);
                        } else {
                            AccountRepository.updateBalance(conn,
                                    target,
                                    random.nextBoolean() ? AccountType.credit : AccountType.checking,
                                    amount);
                        }
                    }

                    return null;
                }, durations::addAll);

        return durations;
    }

    @Override
    public void afterExcution(Output output) {
        AtomicInteger negativeAccounts = new AtomicInteger();

        BigDecimal totalNegative =
                JdbcUtils.execute(dataSource,
                        conn -> {
                            BigDecimal total = BigDecimal.ZERO;
                            try (PreparedStatement ps = conn.prepareStatement(
                                    "SELECT id, sum(balance) total FROM account GROUP BY id")) {
                                try (ResultSet rs = ps.executeQuery()) {
                                    while (rs.next()) {
                                        BigDecimal balance = rs.getBigDecimal(2);
                                        if (balance.compareTo(BigDecimal.ZERO) < 0) {
                                            output.error("Negative balance for account id %s: %,f"
                                                    .formatted(rs.getLong(1), balance));
                                            negativeAccounts.incrementAndGet();
                                            total = total.add(balance);
                                        }
                                    }
                                }
                            }
                            return total;
                        });

        if (negativeAccounts.get() > 0) {
            output.error("You have %d account pairs with a negative total balance! (ノಠ益ಠ)ノ彡┻━┻"
                    .formatted(negativeAccounts.get()));
            output.error("You just lost %s and may want to reconsider your isolation level!! (or use --sfu or --cas)"
                    .formatted(totalNegative));
        } else {
            output.info("You are good! ¯\\_(ツ)_/¯̑̑");
        }
    }
}
