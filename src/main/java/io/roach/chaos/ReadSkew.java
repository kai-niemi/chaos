package io.roach.chaos;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import io.roach.chaos.support.AsciiArt;
import io.roach.chaos.support.JdbcUtils;
import io.roach.chaos.support.TransactionTemplate;
import io.roach.chaos.support.Tuple;

import static io.roach.chaos.AccountRepository.findRandomAccounts;
import static io.roach.chaos.support.RandomData.selectRandom;

public class ReadSkew extends AbstractWorkload {
    private final List<Account> accountSelection = new ArrayList<>();

    private final List<Tuple<Long, BigDecimal>> discrepancies = Collections.synchronizedList(new ArrayList<>());

    private final BlockingQueue<Tuple<Account.Id, Account.Id>> queue = new LinkedBlockingQueue<>(100);

    private final BigDecimal tupleSum = new BigDecimal("1000.00");

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

        this.discrepancies.clear();
    }

    @Override
    public List<Duration> call() {
        List<Duration> durations = new ArrayList<>();

        ThreadLocalRandom random = ThreadLocalRandom.current();

        new TransactionTemplate(dataSource, settings.jitter)
                .executeWithRetries(conn -> {
                    // Drain queue and read each account in tuple separately (rather than using aggregation)
                    Tuple<Account.Id, Account.Id> tuple = queue.poll();
                    while (tuple != null) {
                        Account a = AccountRepository.findById(conn, tuple.getA(), settings.lock);
                        Account b = AccountRepository.findById(conn, tuple.getB(), settings.lock);
                        BigDecimal snapshot = a.getBalance().add(b.getBalance());
                        // Should always observe a constant total
                        if (!snapshot.equals(tupleSum)) {
                            discrepancies.add(Tuple.of(a.getId().getId(), snapshot));
                        }
                        tuple = queue.poll();
                    }

                    Account account = selectRandom(accountSelection);

                    BigDecimal accountSum = AccountRepository.sumAccountBalance(conn, account.getId());

                    BigDecimal amount = BigDecimal.valueOf(random.nextDouble(10, 150))
                            .setScale(2, RoundingMode.HALF_UP);

                    if (accountSum.subtract(amount).compareTo(BigDecimal.ZERO) >= 0) {
                        AccountRepository.addBalance(conn, account.getId().getId(), AccountType.credit,
                                amount.negate());
                        AccountRepository.addBalance(conn, account.getId().getId(), AccountType.checking,
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
                }, durations::addAll);

        return durations;
    }

    @Override
    public void afterExecution(Output output) {
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
                                            output.error("Negative balance for account tuple id: %s (%,f)"
                                                    .formatted(rs.getLong(1), balance));
                                            negativeAccounts.incrementAndGet();
                                            total = total.add(balance);
                                        }
                                    }
                                }
                            }
                            return total;
                        });

        discrepancies
                .stream()
                .limit(10)
                .forEach(tuple -> {
                    output.error("Observed inconsistent sum for account tuple id: %s (%,.2f) expected %,.2f"
                            .formatted(tuple.getA(), tuple.getB(), tupleSum));
                });

        if (discrepancies.isEmpty()) {
            output.info("No sum discrepancies %s"
                    .formatted(AsciiArt.happy()));
        } else {
            output.error("Total sum discrepancies (listing top 10): %d %s"
                    .formatted(discrepancies.size(), AsciiArt.flipTableRoughly()));
        }

        if (negativeAccounts.get() > 0) {
            output.error("You have %d account tuples with a negative total balance! %s"
                    .formatted(negativeAccounts.get(), AsciiArt.flipTableRoughly()));
            output.error("You just lost %s and may want to reconsider your isolation level!! (or use --cas)"
                    .formatted(totalNegative));
        } else {
            output.info("No negative balances %s".formatted(AsciiArt.shrug()));
        }
    }
}
