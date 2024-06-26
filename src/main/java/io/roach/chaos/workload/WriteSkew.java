package io.roach.chaos.workload;

import io.roach.chaos.Settings;
import io.roach.chaos.jdbc.JdbcUtils;
import io.roach.chaos.jdbc.TransactionTemplate;
import io.roach.chaos.util.AsciiArt;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static io.roach.chaos.workload.AccountRepository.findRandomAccounts;
import static io.roach.chaos.util.RandomData.selectRandom;

public class WriteSkew extends AbstractWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private final AtomicInteger accept = new AtomicInteger();

    private final AtomicInteger reject = new AtomicInteger();

    @Override
    public void beforeExecution(Output output, Settings settings) throws Exception {
        super.beforeExecution(output, settings);

        this.accountSelection.addAll(JdbcUtils.execute(settings.getDataSource(),
                conn -> findRandomAccounts(conn, settings.selection)));

        this.accept.set(0);
        this.reject.set(0);
    }

    @Override
    public List<Duration> call() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        List<Duration> durations = new ArrayList<>();

        new TransactionTemplate(settings.getDataSource(), settings.jitter, !settings.skipRetry)
                .execute(conn -> {
                    Account target = selectRandom(accountSelection);

                    BigDecimal amount = BigDecimal.valueOf(random.nextDouble(1, 50))
                            .setScale(2, RoundingMode.HALF_UP);

                    // Invariant check - can't use SFU
                    BigDecimal totalBalance = AccountRepository.sumAccountBalance(conn, target.getId());
                    if (totalBalance.subtract(amount).compareTo(BigDecimal.ZERO) > 0) {
                        accept.incrementAndGet();
                        // Skew point where different threads may pick different paths
                        // (allowed in snapshot and RC but not in 1SR)
                        if (settings.cas) {
                            AccountRepository.addBalanceCAS(conn,
                                    target.getId().getId(),
                                    random.nextBoolean() ? AccountType.credit : AccountType.checking,
                                    amount.negate(),
                                    target.getVersion());
                        } else {
                            AccountRepository.addBalance(conn,
                                    target.getId().getId(),
                                    random.nextBoolean() ? AccountType.credit : AccountType.checking,
                                    amount.negate());
                        }
                    } else {
                        reject.incrementAndGet();
                    }

                    return null;
                }, durations::addAll);

        return durations;
    }

    @Override
    public void afterExecution(Output output, Exporter exporter) {
        output.column("Balance update accepts:", "%d".formatted(accept.get()));
        output.column("Balance update rejects:", "%d".formatted(reject.get()));

        AtomicInteger negativeAccounts = new AtomicInteger();

        BigDecimal totalNegative =
                JdbcUtils.execute(settings.getDataSource(),
                        conn -> {
                            BigDecimal total = BigDecimal.ZERO;
                            try (PreparedStatement ps = conn.prepareStatement(
                                    "SELECT id, sum(balance) total FROM account GROUP BY id")) {
                                try (ResultSet rs = ps.executeQuery()) {
                                    while (rs.next()) {
                                        BigDecimal balance = rs.getBigDecimal(2);
                                        if (balance.compareTo(BigDecimal.ZERO) < 0) {
                                            output.error("Negative balance for account pair id: %s (%,f)"
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
            output.error("You have %d account tuples with a negative total balance! %s"
                    .formatted(negativeAccounts.get(), AsciiArt.flipTableRoughly()));
            output.error("You just lost %s and may want to reconsider your isolation level!! (or use --cas)"
                    .formatted(totalNegative));
        } else {
            output.info("You are good! %s".formatted(AsciiArt.shrug()));
        }

        exporter.write(List.of("discrepancies", (long) negativeAccounts.get(), "counter"));
    }
}
