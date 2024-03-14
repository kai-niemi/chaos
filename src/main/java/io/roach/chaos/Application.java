package io.roach.chaos;

import io.roach.chaos.jdbc.JdbcUtils;
import io.roach.chaos.util.AnsiColor;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.Multiplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static void printUsageAndQuit(String reason) {
        output.headerHighlight("Usage: java -jar chaos.jar [options] <workload>");
        output.info("");
        output.header("Common options:");
        output.columnLeft("--help", "this help");
        output.columnLeft("--rc", "read-committed isolation", "(1SR)");
        output.columnLeft("--cas", "optimistic locking using CAS", "(false)");
        output.columnLeft("--sfu", "pessimistic locking using select-for-update", "(false)");
        output.columnLeft("--debug,--trace", "verbose SQL trace logging", "(false)");
        output.columnLeft("--export", "export results to chaos.csv file", "(false)");
        output.columnLeft("--skip-create", "skip DDL create script at startup", "(false)");
        output.columnLeft("--skip-init", "skip DML init script at startup", "(false)");
        output.columnLeft("--skip-retry", "skip client-side retries", "(false)");
        output.columnLeft("--jitter", "enable exponential backoff jitter", "(false)");
        output.info("  Hint: skip jitter for more comparable results between isolation levels.");
        output.columnLeft("--dialect <db>", "database dialect ", "crdb|psql (crdb)");
        output.columnLeft("--threads <num>", "max number of threads", "(host vCPUs x 2)");
        output.columnLeft("--iterations <num>", "number of cycles to run", "(1K)");
        output.columnLeft("--accounts <num>", "number of accounts to create and randomize between", "(50K)");
        output.columnLeft("--selection <num>", "random selection of accounts to pick between", "(500)");
        output.info("  Hint: decrease selection to observe anomalies in --rc.");
        output.info("");

        output.header("Lost update workload options:");
        output.columnLeft("--contention <num>", "contention level", "(8, must be a multiple of 2)");
        output.info("");

        output.header("Connection options include:");
        output.columnLeft("--url", "datasource URL", "(jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable)");
        output.columnLeft("--user", "datasource user name", "(root)");
        output.columnLeft("--password", "datasource password", "(<empty>)");
        output.info("");

        output.header("Workload one of:");
        EnumSet.allOf(WorkloadType.class).forEach(
                workloadType -> output.columnLeft(workloadType.name(), workloadType.note()));

        output.info("");

        output.error(reason);

        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Settings settings = parse(args);

        final DataSource dataSource = settings.getDataSource();

        JdbcUtils.execute(dataSource, conn -> {
            JdbcUtils.inspectDatabaseMetadata(conn, (k, v) -> output.column(k + ":", "%s".formatted(v)));
            return null;
        });

        final String version = JdbcUtils.execute(dataSource,
                conn -> JdbcUtils.selectOne(conn, "SELECT version()", String.class));
        final String driver = JdbcUtils.execute(dataSource, JdbcUtils::driverVersion);
        final String isolationLevel = JdbcUtils.execute(dataSource,
                conn -> JdbcUtils.selectOne(conn, "SHOW transaction_isolation", String.class));

        if (settings.readCommitted && !"read committed".equalsIgnoreCase(isolationLevel)) {
            logger.error("Read-committed is enabled but database default is '%s'".formatted(isolationLevel));
            logger.error("Execute: SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = 'true';");
            System.exit(1);
        }

        if (settings.selection > settings.numAccounts) {
            logger.warn("Selection (%d) larger than number of accounts (%d)!"
                    .formatted(settings.selection, settings.numAccounts));
            settings.selection = settings.numAccounts;
        }

        output.header("Workload Overview");
        output.column("Isolation level:", "%s".formatted(isolationLevel));
        output.column("Workload:", "%s".formatted(settings.workloadType.name()));
        output.column("Threads:", "%d".formatted(settings.workers));
        output.column("Accounts:", "%d".formatted(settings.numAccounts));
        output.column("Selection:", "%d (%.1f%%)"
                .formatted(settings.selection,
                        (double) settings.selection / (double) settings.numAccounts * 100.0));

        Workload workload = settings.workloadType.getFactory().get();
        workload.beforeExecution(output, settings);

        final ExecutorService executorService = Executors.newFixedThreadPool(settings.workers);
        final Deque<Future<List<Duration>>> futures = new ArrayDeque<>();

        output.info("");
        output.info("Queuing max %d workers for %d iterations - let it rip! %s"
                .formatted(settings.workers, settings.iterations, AsciiArt.happy()));

        final Instant startTime = Instant.now();
        IntStream.rangeClosed(1, settings.iterations)
                .forEach(value -> futures.add(executorService.submit(workload)));

        int commits = 0;
        int fails = 0;
        final List<Duration> allDurations = new ArrayList<>();
        final AtomicInteger totalRetries = new AtomicInteger();

        while (!futures.isEmpty()) {
            AsciiArt.printProgressBar(settings.iterations, settings.iterations - futures.size(),
                    futures.size() + " futures remain");

            try {
                List<Duration> stats = futures.pop().get();

                totalRetries.addAndGet(stats.size() - 1); // More than one duration means at least one retry
                allDurations.addAll(stats);

                commits++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace(System.err);
                fails++;
                break;
            } catch (ExecutionException e) { // Mainly if retries are exhausted
                logger.error("", e.getCause());
                fails++;
            }
        }

        final Instant stopTime = Instant.now();

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

        executorService.shutdownNow();

        output.info("");
        output.header("Totals");
        output.column("Args:", "%s".formatted(Arrays.stream(args).toList()));
        output.column("Using:", "%s".formatted(version));
        output.column("Driver:", "%s".formatted(driver));
        output.column("Threads:", "%s".formatted(settings.workers));
        output.column("Contention level:", "%s".formatted(settings.level));
        output.column("Account selection:", "%,d of %,d".formatted(settings.selection, settings.numAccounts));
        output.column("Execution time:", "%s".formatted(Duration.between(startTime, stopTime)));
        output.column("Total commits:", "%,d".formatted(commits));
        output.column("Total fails:", "%,d".formatted(fails));
        output.column("Total retries:", "%,d".formatted(totalRetries.get()));

        output.header("Timings");
        output.column("Avg time in txn:", "%.1f ms".formatted(summaryStatistics.getAverage()));
        output.column("Cumulative time in txn:", "%.0f ms".formatted(summaryStatistics.getSum()));
        output.column("Min time in txn:", "%.1f ms".formatted(summaryStatistics.getMin()));
        output.column("Max time in txn:", "%.1f ms".formatted(summaryStatistics.getMax()));
        output.column("Total samples:", "%d".formatted(summaryStatistics.getCount()));
        output.column("P50 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .50)));
        output.column("P95 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .95)));
        output.column("P99 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .99)));
        output.column("P999 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .999)));

        output.header("Safety");
        output.column("Used locks (sfu):", "%s".formatted(settings.lock ? "yes" : "no"));
        output.column("Used CAS:", "%s".formatted(settings.cas ? "yes" : "no"));
        output.column("Isolation level:", "%s".formatted(isolationLevel));

        if (fails > 0) {
            output.error("There are %d non-transient errors that may invalidate the final outcome!".formatted(fails));
        }

        output.header("Outcome");

        if (settings.export) {
            try (Exporter exporter = new CsvExporter(Path.of("chaos.csv"))) {
                exporter.writeHeader(List.of("name", "value", "unit"));
                exporter.write(List.of("duration", Duration.between(startTime, stopTime), "time"));
                exporter.write(List.of("threads", settings.workers, "counter"));
                exporter.write(List.of("level", settings.level, "counter"));
                exporter.write(List.of("selection", settings.selection, "counter"));
                exporter.write(List.of("accounts", settings.numAccounts, "counter"));
                exporter.write(List.of("commits", commits, "counter"));
                exporter.write(List.of("fails", fails, "counter"));
                exporter.write(List.of("retries", totalRetries.get(), "counter"));
                exporter.write(List.of("avgTime", summaryStatistics.getAverage(), "ms"));
                exporter.write(List.of("cumulativeTime", summaryStatistics.getSum(), "ms"));
                exporter.write(List.of("minTime", summaryStatistics.getMin(), "ms"));
                exporter.write(List.of("maxTime", summaryStatistics.getMax(), "ms"));
                exporter.write(List.of("samples", summaryStatistics.getCount(), "counter"));
                exporter.write(List.of("P50", percentile(allDurationMillis, .50), "ms"));
                exporter.write(List.of("P95", percentile(allDurationMillis, .95), "ms"));
                exporter.write(List.of("P99", percentile(allDurationMillis, .99), "ms"));
                exporter.write(List.of("P999", percentile(allDurationMillis, .999), "ms"));

                workload.afterExecution(output, exporter);
            }
        } else {
            workload.afterExecution(output, () -> {
            });
        }
    }

    public static Settings parse(String[] args) {
        Settings settings = new Settings();

        LinkedList<String> argsList = new LinkedList<>(List.of(args));
        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.startsWith("--")) {
                if (arg.equals("--debug") || arg.equals("--trace")) {
                    settings.debugProxy = true;
                } else if (arg.equals("--dialect")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.dialect = Dialect.valueOf(argsList.pop());
                } else if (arg.equals("--jitter")) {
                    settings.jitter = true;
                } else if (arg.equals("--export")) {
                    settings.export = true;
                } else if (arg.equals("--skip-create")) {
                    settings.skipCreate = true;
                } else if (arg.equals("--skip-init")) {
                    settings.skipInit = true;
                } else if (arg.equals("--skip-retry")) {
                    settings.skipRetry = true;
                } else if (arg.equals("--rc") || arg.equals("--read-committed")) {
                    settings.readCommitted = true;
                } else if (arg.equals("--sfu") || arg.equals("--select-for-update")) {
                    settings.lock = true;
                } else if (arg.equals("--cas") || arg.equals("--compare-and-set")) {
                    settings.cas = true;
                } else if (arg.equals("--contention")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.level = Integer.parseInt(argsList.pop());
                    if (settings.level % 2 != 0) {
                        printUsageAndQuit("Contention level must be a multiple of 2");
                    }
                } else if (arg.equals("--threads")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.workers = Integer.parseInt(argsList.pop());
                    if (settings.workers <= 0) {
                        printUsageAndQuit("Workers must be > 0");
                    }
                } else if (arg.equals("--iterations")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.iterations = Multiplier.parseInt(argsList.pop());
                    if (settings.iterations <= 0) {
                        printUsageAndQuit("Iterations must be > 0");
                    }
                } else if (arg.equals("--accounts")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.numAccounts = Multiplier.parseInt(argsList.pop());
                    if (settings.numAccounts <= 0) {
                        printUsageAndQuit("Accounts must be > 0");
                    }
                } else if (arg.equals("--selection")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.selection = Multiplier.parseInt(argsList.pop());
                    if (settings.selection <= 0) {
                        printUsageAndQuit("Selection must be > 0");
                    }
                } else if (arg.equals("--url")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.url = argsList.pop();
                } else if (arg.equals("--dev")) {
                    settings.url = "jdbc:postgresql://192.168.1.99:26257/defaultdb?sslmode=disable";
                } else if (arg.equals("--dev2")) {
                    settings.url = "jdbc:cockroachdb://192.168.1.99:26257/defaultdb?sslmode=disable";
                } else if (arg.equals("--user")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.user = argsList.pop();
                } else if (arg.equals("--password")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value for " + arg);
                    }
                    settings.password = argsList.pop();
                } else if (arg.equals("--help")) {
                    printUsageAndQuit("");
                } else {
                    printUsageAndQuit("Unknown option: " + arg);
                }
            } else {
                try {
                    settings.workloadType = WorkloadType.valueOf(arg);
                } catch (IllegalArgumentException e) {
                    printUsageAndQuit("Unknown arg: " + arg);
                }
            }
        }
        return settings;
    }

    private static double percentile(List<Double> orderedList, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException(">=0 N <=1");
        }
        if (!orderedList.isEmpty()) {
            int index = (int) Math.ceil(percentile * orderedList.size());
            return orderedList.get(index - 1);
        }
        return 0f;
    }

    private static final Output output = new Output() {
        @Override
        public void header(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.BOLD_BRIGHT_CYAN.getCode(), text, AnsiColor.RESET.getCode());
        }

        @Override
        public void headerHighlight(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.BOLD_BRIGHT_WHITE.getCode(), text, AnsiColor.RESET.getCode());
        }

        @Override
        public void column(String prefix, String suffix) {
            System.out.printf("%s%30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), prefix);
            System.out.printf("%s%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), suffix, AnsiColor.RESET.getCode());
            System.out.println();
        }

        @Override
        public void columnLeft(String prefix, String suffix) {
            System.out.printf("%s%-30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), prefix);
            System.out.printf("%s%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), suffix, AnsiColor.RESET.getCode());
            System.out.println();
        }

        @Override
        public void columnLeft(String col1, String col2, String col3) {
            System.out.printf("%s%-30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
            System.out.printf("%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2);
            System.out.printf("%s %s%s", AnsiColor.BOLD_BRIGHT_PURPLE.getCode(), col3, AnsiColor.RESET.getCode());
            System.out.println();
        }

        @Override
        public void info(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.YELLOW.getCode(), text, AnsiColor.RESET.getCode());
        }

        @Override
        public void error(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.BOLD_BRIGHT_RED.getCode(), text, AnsiColor.RESET.getCode());
        }
    };
}
