package io.roach.chaos;

import io.roach.chaos.support.AnsiColor;
import io.roach.chaos.support.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
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
        System.out.println("Usage: java -jar chaos.jar [options]");
        System.out.println();
        System.out.println("Either --sfu or --cas required for correct execution in RC.");
        System.out.println("Neither --sfu or --cas required for correct execution in 1SR.");
        System.out.println();
        System.out.println("--workload <type>     workload type either 'lost_update' or 'write_skew' (default is lost_update)");
        System.out.println();
        System.out.println("Common workload options:");
        System.out.println("--rc                  read-committed isolation (default is 1SR)");
        System.out.println("--cas                 optimistic locking using CAS (default is false)");
        System.out.println("--debug               verbose SQL trace logging (default is false)");
        System.out.println("--skip-create         skip creation of schema and test data (default is false)");
        System.out.println("--jitter              enable exponential backoff jitter (default is false)");
        System.out.println("                      Skip the jitter for more comparable results between isolation levels.");
        System.out.println("--threads <num>       max number of threads (default is # host vCPUs x 2)");
        System.out.println("--iterations <num>    number of cycles to run (default is 1,000)");
        System.out.println("--accounts <num>      number of accounts to create and randomize between (default is 50K)");
        System.out.println("--selection <num>     number of accounts to randomize between (default is 500 or 1% of accounts)");
        System.out.println();

        System.out.println("Lost Update options:");
        System.out.println("--sfu                 pessimistic locking using select-for-update (default is false)");
        System.out.println("--contention <level>  contention level (default is 8, must be a multiple of 2)");
        System.out.println();

        System.out.println("Connection options include:");
        System.out.println("--url                 datasource URL (jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable)");
        System.out.println("--user                datasource user name (root)");
        System.out.println("--password            datasource password (<empty>)");
        System.out.println();
        System.out.println(reason);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Settings settings = new Settings();
        WorkloadType workloadType = WorkloadType.lost_update;
//        WorkloadType workloadType = WorkloadType.write_skew;

        LinkedList<String> argsList = new LinkedList<>(List.of(args));
        while (!argsList.isEmpty()) {
            String arg = argsList.pop();
            if (arg.startsWith("--")) {
                if (arg.equals("--debug")) {
                    settings.debugProxy = true;
                } else if (arg.equals("--skip-create")) {
                    settings.skipCreate = true;
                } else if (arg.equals("--jitter")) {
                    settings.jitter = true;
                } else if (arg.equals("--rc") || arg.equals("--read-committed")) {
                    settings.readCommitted = true;
                } else if (arg.equals("--sfu") || arg.equals("--select-for-update")) {
                    settings.lock = true;
                } else if (arg.equals("--cas") || arg.equals("--compare-and-set")) {
                    settings.cas = true;
                } else if (arg.equals("--contention")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.level = Integer.parseInt(argsList.pop());
                    if (settings.level % 2 != 0) {
                        printUsageAndQuit("Contention level must be a multiple of 2");
                    }
                } else if (arg.equals("--threads")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.workers = Integer.parseInt(argsList.pop());
                    if (settings.workers <= 0) {
                        printUsageAndQuit("Workers must be > 0");
                    }
                } else if (arg.equals("--iterations")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.iterations = Integer.parseInt(argsList.pop());
                    if (settings.iterations <= 0) {
                        printUsageAndQuit("Iterations must be > 0");
                    }
                } else if (arg.equals("--accounts")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.numAccounts = Integer.parseInt(argsList.pop());
                    if (settings.numAccounts <= 0) {
                        printUsageAndQuit("Accounts must be > 0");
                    }
                } else if (arg.equals("--selection")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.selection = Integer.parseInt(argsList.pop());
                    if (settings.selection <= 0) {
                        printUsageAndQuit("Selection must be > 0");
                    }
                } else if (arg.equals("--workload")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    workloadType = WorkloadType.valueOf(argsList.pop());
                } else if (arg.equals("--url")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.url = argsList.pop();
                } else if (arg.equals("--user")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.user = argsList.pop();
                } else if (arg.equals("--password")) {
                    if (argsList.isEmpty()) {
                        printUsageAndQuit("Expected value");
                    }
                    settings.password = argsList.pop();
                } else if (arg.equals("--help")) {
                    printUsageAndQuit("");
                } else {
                    printUsageAndQuit("Unknown parameter: " + arg);
                }
            } else {
                printUsageAndQuit("Unknown arg: " + arg);
            }
        }

        final DataSource dataSource = settings.createDataSource();

        final String version = JdbcUtils.execute(dataSource,
                conn -> JdbcUtils.selectOne(conn, "SELECT version()", String.class));
        final String isolationLevel = JdbcUtils.execute(dataSource,
                conn -> JdbcUtils.selectOne(conn, "SHOW transaction_isolation", String.class));

        if (settings.readCommitted && !"read committed".equalsIgnoreCase(isolationLevel)) {
            output.error("Read-committed is enabled but database default is '%s'".formatted(isolationLevel));
        }

        if (settings.selection > settings.numAccounts) {
            output.error("Selection (%d) larger than number of accounts (%d)!"
                    .formatted(settings.selection, settings.numAccounts));
            settings.selection = settings.numAccounts;
        }

        Workload workload = workloadType.getFactory().get();
        workload.setup(settings, dataSource);

        output.header("Ramping Up");
        output.info("Database: %s".formatted(version));
        output.info("Isolation level: %s".formatted(isolationLevel));
        output.info("Workload: %s".formatted(workloadType.name()));
        output.info("Threads: %d".formatted(settings.workers));
        output.info("Accounts: %d".formatted(settings.numAccounts));
        output.info("Selection: %d".formatted(settings.selection));

        final ExecutorService executorService = Executors.newFixedThreadPool(settings.workers);

        workload.beforeExecution(output);

        final Instant startTime = Instant.now();

        output.info("Queuing %d workers for %d iterations - please hold"
                .formatted(settings.workers, settings.iterations));

        final Deque<Future<List<Duration>>> futures = new ArrayDeque<>();

        IntStream.rangeClosed(1, settings.iterations)
                .forEach(value -> futures.add(executorService.submit(workload)));

        int commits = 0;
        int fail = 0;
        final List<Duration> allDurations = new ArrayList<>();
        final AtomicInteger totalRetries = new AtomicInteger();

        while (!futures.isEmpty()) {
            output.debug("Awaiting completion (%d futures remain)".formatted(futures.size()));

            try {
                List<Duration> stats = futures.pop().get();

                totalRetries.addAndGet(stats.size() - 1); // More than one duration means at least one retry
                allDurations.addAll(stats);

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

        output.header("Totals");
        output.info("Args: %s".formatted(Arrays.stream(args).toList()));
        output.info("Using: %s".formatted(version));
        output.info("Threads: %s".formatted(settings.workers));
        output.info("Contention level: %s".formatted(settings.level));
        output.info("Account selection: %,d of %,d".formatted(settings.selection, settings.numAccounts));
        output.info("Execution time: %s".formatted(Duration.between(startTime, stopTime)));
        output.info("Total commits: %,d".formatted(commits));
        output.info("Total fails: %,d".formatted(fail));
        output.info("Total retries: %,d".formatted(totalRetries.get()));

        output.header("Timings");
        output.info("Avg time spent in txn: %.1f ms".formatted(summaryStatistics.getAverage()));
        output.info("Cumulative time spent in txn: %.0f ms".formatted(summaryStatistics.getSum()));
        output.info("Min time in txn: %.1f ms".formatted(summaryStatistics.getMin()));
        output.info("Max time in txn: %.1f ms".formatted(summaryStatistics.getMax()));
        output.info("Tot samples: %d".formatted(summaryStatistics.getCount()));
        output.info("P50 latency %.1f ms".formatted(percentile(allDurationMillis, .50)));
        output.info("P95 latency %.1f ms".formatted(percentile(allDurationMillis, .95)));
        output.info("P99 latency %.1f ms".formatted(percentile(allDurationMillis, .99)));
        output.info("P999 latency %.1f ms".formatted(percentile(allDurationMillis, .999)));

        output.header("Correctness");
        output.info("Using locks (sfu): %s".formatted(settings.lock ? "yes" : "no"));
        output.info("Using CAS: %s".formatted(settings.cas ? "yes" : "no"));
        output.info("Isolation level: %s".formatted(isolationLevel));
        
        if (fail > 0) {
            output.error("There are errors: %d".formatted(fail));
        }

        output.header("Outcome");
        workload.afterExcution(output);

        executorService.shutdownNow();
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
            System.out.printf("%s<<%s>>%s\n", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), text, AnsiColor.RESET.getCode());
        }

        @Override
        public void debug(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.BLUE.getCode(), text, AnsiColor.RESET.getCode());
        }

        @Override
        public void info(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.YELLOW.getCode(), text, AnsiColor.RESET.getCode());
        }

        @Override
        public void error(String text) {
            System.out.printf("%s%s%s\n", AnsiColor.RED.getCode(), text, AnsiColor.RESET.getCode());
        }
    };
}
