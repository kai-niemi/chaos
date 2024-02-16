package io.roach.chaos;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import io.roach.chaos.support.ConnectionTemplate;
import io.roach.chaos.support.TransactionCallback;
import io.roach.chaos.support.TransactionTemplate;
import io.roach.chaos.support.Tuple;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static void printUsageAndQuit(String reason) {
        System.out.println("Usage: java -jar chaos.jar [options]");
        System.out.println();
        System.out.println("Either --sfu or --cas required for correct execution in RC.");
        System.out.println("Neither --sfu or --cas required for correct execution in 1SR.");
        System.out.println();
        System.out.println("Workload options:");
        System.out.println("--rc                  read-committed isolation (default is 1SR)");
        System.out.println("--sfu                 pessimistic locking using select-for-update (default is false)");
        System.out.println("--cas                 optimistic locking using CAS (default is false)");
        System.out.println("--debug               verbose SQL trace logging (default is false)");
        System.out.println("--skip-create         skip creation of schema and test data (default is false)");
        System.out.println("--threads <num>       number of threads (default is # host vCPUs)");
        System.out.println("--iterations <num>    number of cycles to run (default is 1K)");
        System.out.println("--accounts <num>      number of accounts to create and randomize between (default is 50K)");
        System.out.println("--selection <num>     number of accounts to randomize between (default is 500 or 1%)");
        System.out.println("--contention <level>  contention level (default is 6, must be multiple of 2)");
        System.out.println();
        System.out.println("Connection options:");
        System.out.println("--url                 datasource URL");
        System.out.println("--user                datasource user name");
        System.out.println("--password            datasource password");

        System.out.println();
        System.out.println(reason);

        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
//        String url = "jdbc:postgresql://192.168.1.99:26257/test?sslmode=disable";
        String url = "jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable";
        String user = "root";
        String password = "";

        final AtomicBoolean lock = new AtomicBoolean(false);
        final AtomicBoolean cas = new AtomicBoolean(false);
        boolean readCommitted = false;
        boolean debugProxy = false;
        boolean skipCreate = false;

        int workers = Runtime.getRuntime().availableProcessors();
        final AtomicInteger level = new AtomicInteger(8);
        final AtomicInteger numAccounts = new AtomicInteger(50_000);
        final AtomicInteger selection = new AtomicInteger(500);
        int iterations = 1000;

        LinkedList<String> argsList = new LinkedList<>(List.of(args));
        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.startsWith("--")) {
                if (arg.equals("--debug")) {
                    debugProxy = true;
                } else if (arg.equals("--skip-create")) {
                    skipCreate = true;
                } else if (arg.equals("--rc") || arg.equals("--read-committed")) {
                    readCommitted = true;
                } else if (arg.equals("--1sr") || arg.equals("--serializable")) {
                    readCommitted = false;
                } else if (arg.equals("--sfu") || arg.equals("--select-for-update")) {
                    lock.set(true);
                } else if (arg.equals("--cas") || arg.equals("--compare-and-set")) {
                    cas.set(true);
                } else if (arg.equals("--contention")) {
                    level.set(Integer.parseInt(argsList.pop()));
                    if (level.get() % 2 != 0) {
                        printUsageAndQuit("Contention level must be a multiple of 2");
                    }
                } else if (arg.equals("--threads")) {
                    workers = Integer.parseInt(argsList.pop());
                } else if (arg.equals("--iterations")) {
                    iterations = Integer.parseInt(argsList.pop());
                } else if (arg.equals("--accounts")) {
                    numAccounts.set(Integer.parseInt(argsList.pop()));
                } else if (arg.equals("--selection")) {
                    selection.set(Integer.parseInt(argsList.pop()));
                } else if (arg.equals("--url")) {
                    url = argsList.pop();
                } else if (arg.equals("--user")) {
                    user = argsList.pop();
                } else if (arg.equals("--password")) {
                    password = argsList.pop();
                } else if (arg.equals("--help")) {
                    printUsageAndQuit("");
                } else {
                    printUsageAndQuit("Unknown parameter: " + arg);
                }
            } else {
                printUsageAndQuit("Unknown arg");
            }
        }

        // Let's go

        final HikariDataSource hikariDS = new HikariDataSource();
        hikariDS.setJdbcUrl(url);
        hikariDS.setUsername(user);
        hikariDS.setPassword(password);
        hikariDS.setAutoCommit(true);
        hikariDS.setMaximumPoolSize(workers);
        hikariDS.setMinimumIdle(workers);
        hikariDS.setTransactionIsolation(readCommitted ? "TRANSACTION_READ_COMMITTED" : "TRANSACTION_SERIALIZABLE");

        final DataSource dataSource = debugProxy ?
                ProxyDataSourceBuilder
                        .create(hikariDS)
                        .asJson()
                        .multiline()
                        .logQueryBySlf4j()
                        .build()
                : hikariDS;

        if (!skipCreate) {
            logger.info("Creating schema");
            SchemaSupport.setupSchema(dataSource);

            logger.info("Creating %,d accounts in batches".formatted(numAccounts.get()));
            ConnectionTemplate.execute(dataSource, conn -> {
                SchemaSupport.deleteAccounts(conn);
                SchemaSupport.createAccounts(conn, new BigDecimal("5000.00"), numAccounts.get());
                return null;
            });
        }

        logger.info("Finding random selection of %,d accounts (%f%%)"
                .formatted(selection.get(), selection.get() / numAccounts.get() * 100.0));
        final List<Long> ids = ConnectionTemplate.execute(dataSource,
                conn -> Repository.findRandomIDs(conn, selection.get()));

        final BigDecimal initialBalance = ConnectionTemplate.execute(dataSource, Repository::readTotalBalance);
        final String isolationLevel = ConnectionTemplate.execute(dataSource, SchemaSupport::showIsolationLevel);

        final ExecutorService executorService = Executors.newFixedThreadPool(workers);
        final Deque<Future<Void>> futures = new ArrayDeque<>();
        final Instant startTime = Instant.now();

        final List<Duration> allDurations = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger totalRetries = new AtomicInteger();

        final Consumer<List<Duration>> stats = (durations) -> {
            totalRetries.addAndGet(durations.size() - 1); // More than one duration means at least one retry
            allDurations.addAll(durations);
        };

        logger.info("Queuing %d workers for %d iterations - let the games being".formatted(workers, iterations));

        IntStream.rangeClosed(1, iterations)
                .forEach(value -> futures.add(executorService.submit(() -> {
                            ThreadLocalRandom random = ThreadLocalRandom.current();

                            List<Tuple<Long, BigDecimal>> legs = new ArrayList<>();
                            Set<Long> consumed = new HashSet<>();

                            IntStream.rangeClosed(1, Integer.MAX_VALUE)
                                    .takeWhile(v -> legs.size() != level.get())
                                    .forEach(leg -> {
                                        long from = selectRandom(ids);
                                        long to = selectRandom(ids);
                                        if (consumed.add(from) && consumed.add(to)) {
                                            BigDecimal amt = new BigDecimal(random.nextDouble(1, 10))
                                                    .setScale(2, RoundingMode.HALF_UP);
                                            legs.add(Tuple.of(from, amt));
                                            legs.add(Tuple.of(to, amt.negate()));
                                        }
                                    });

                            TransactionTemplate.executeWithRetries(dataSource,
                                    transfer(legs, lock.get(), cas.get()), stats);

                            return null;
                        }
                )));

        int commits = 0;
        int fail = 0;

        while (!futures.isEmpty()) {
            logger.info("Awaiting completion (%d futures remain)".formatted(futures.size()));
            try {
                futures.pop().get();
                commits++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace(System.err);
                fail++;
                break;
            } catch (ExecutionException e) { // Mainly if retries are exhausted
                logger.error("", e.getCause());
                fail++;
            }
        }

        executorService.shutdownNow();

        final Instant stopTime = Instant.now();

        final BigDecimal finalBalance = ConnectionTemplate.execute(dataSource, Repository::readTotalBalance);

        final DoubleSummaryStatistics summaryStatistics = allDurations
                .stream()
                .mapToDouble(Duration::toMillis)
                .sorted()
                .summaryStatistics();
        final List<Double> allDurationMillis = allDurations.stream()
                .mapToDouble(Duration::toMillis)
                .sorted()
                .boxed()
                .toList();

        logger.info("All workers complete");

        System.out.println("Totals >>");
        System.out.println("  Execution time: %s".formatted(Duration.between(startTime, stopTime)));
        System.out.println("  Total commits: %d".formatted(commits));
        System.out.println("  Total fails: %d".formatted(fail));
        System.out.println("  Total retries: %d".formatted(totalRetries.get()));

        System.out.println("Timings >>");
        System.out.println("  Avg time spent in txn: %.2f ms".formatted(summaryStatistics.getAverage()));
        System.out.println("  Cumulative time spent in txn: %.2f ms".formatted(summaryStatistics.getSum()));
        System.out.println("  Min time in txn: %.2f ms".formatted(summaryStatistics.getMin()));
        System.out.println("  Max time in txn: %.2f ms".formatted(summaryStatistics.getMax()));
        System.out.println("  Tot samples: %d".formatted(summaryStatistics.getCount()));
        System.out.println("  P95 latency %.0f ms".formatted(percentile(allDurationMillis, .95)));
        System.out.println("  P99 latency %.0f ms".formatted(percentile(allDurationMillis, .99)));
        System.out.println("  P99.9 latency %.0f ms".formatted(percentile(allDurationMillis, .999)));

        System.out.println("Correctness >>");
        System.out.println("  Using locks (sfu): %s".formatted(lock.get() ? "yes" : "no"));
        System.out.println("  Using CAS: %s".formatted(cas.get() ? "yes" : "no"));
        System.out.println("  Isolation level: %s".formatted(isolationLevel));

        System.out.println("Verdict >>");
        System.out.println("  Total initial balance: %s".formatted(initialBalance));
        System.out.println("  Total final balance: %s".formatted(finalBalance));

        if (!initialBalance.equals(finalBalance)) {
            System.out.println("%s != %s (ノಠ益ಠ)ノ彡┻━┻"
                    .formatted(initialBalance, finalBalance));
            System.out.println(
                    "You just lost %s and may want to reconsider your isolation level!! (or use --sfu or --cas)"
                            .formatted(initialBalance.subtract(finalBalance)));
        } else {
            System.out.println("  You are good! ¯\\_(ツ)_/¯̑̑");
        }
    }

    private static TransactionCallback<Void> transfer(List<Tuple<Long, BigDecimal>> legs,
                                                      boolean lock,
                                                      boolean cas) {
        return conn -> {
            BigDecimal checksum = BigDecimal.ZERO;

            for (Tuple<Long, BigDecimal> leg : legs) {
                Tuple<BigDecimal, Integer> balance = Repository.readBalance(conn, leg.getA(), lock);

                if (cas) {
                    Repository.updateBalanceWithCAS(conn, leg.getA(),
                            balance.getA().add(leg.getB()), balance.getB());
                } else {
                    Repository.updateBalance(conn, leg.getA(),
                            balance.getA().add(leg.getB()));
                }

                checksum = checksum.add(leg.getB());
            }

            if (checksum.compareTo(BigDecimal.ZERO) != 0) {
                throw new IllegalArgumentException(
                        "Sum of account legs must equal 0 (got " + checksum.toPlainString() + ")"
                );
            }

            return null;
        };
    }

    private static double percentile(List<Double> orderedList, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException(">=0 N <=1");
        }
        if (!orderedList.isEmpty()) {
            int index = (int) Math.ceil(percentile * orderedList.size());
            return orderedList.get(index - 1);
        }
        return 0;
    }

    private static <E> E selectRandom(List<E> collection) {
        return collection.get(ThreadLocalRandom.current().nextInt(collection.size()));
    }
}
