package io.roach.chaos;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jdbc.JdbcRepositoriesAutoConfiguration;

import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.ColoredLogger;
import io.roach.chaos.util.CsvExporter;
import io.roach.chaos.util.DatabaseInfo;
import io.roach.chaos.util.Exporter;
import io.roach.chaos.workload.Workload;

@SpringBootApplication(exclude = {
        JdbcRepositoriesAutoConfiguration.class
})
public class Application implements ApplicationRunner {
    private final ColoredLogger logger = ColoredLogger.newInstance();

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Settings settings;

    @Autowired
    private Workload workload;

    @Override
    public void run(ApplicationArguments args) {
        if (settings.getSelection() > settings.getNumAccounts()) {
            logger.warn("Selection (%d) larger than number of accounts (%d)!"
                    .formatted(settings.getSelection(), settings.getNumAccounts()));
            settings.setSelection(settings.getNumAccounts());
        }

        if (settings.getWorkers() <= 0) {
            settings.setWorkers(Runtime.getRuntime().availableProcessors() * 2);
            logger.warn("Setting max threads to %d".formatted(settings.getWorkers()));
        }

        workload.validateSettings();

        printSettings(args);

        if (settings.isQuit()) {
            logger.info("Quitting..");
            System.exit(0);
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(settings.getWorkers());
        final List<Duration> allDurations = new ArrayList<>();
        final AtomicInteger totalRetries = new AtomicInteger();
        int commits = 0;
        int fails = 0;

        final Progress progress = new Progress()
                .setStartTime(Instant.now())
                .setTotal(settings.getIterations());

        try {
            workload.beforeAllExecutions();

            final Instant startTime = Instant.now();

            final Deque<Future<List<Duration>>> futures = new ArrayDeque<>();

            // Queue workers
            IntStream.rangeClosed(1, settings.getIterations())
                    .forEach(value -> futures.add(executorService.submit(workload::oneExecution)));

            // Await completion
            while (!futures.isEmpty()) {
                progress.setCurrent(settings.getIterations() - futures.size());
                progress.setLabel("[%,d futures remain]".formatted(futures.size()));

                AsciiArt.printProgressBar(
                        progress.getTotal(),
                        progress.getCurrent(),
                        progress.getLabel(),
                        progress.getCallsPerSec(),
                        progress.getRemainingMillis());

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

            printResults(Duration.between(startTime, Instant.now()),
                    commits, fails, totalRetries.get(), allDurations);
            workload.afterAllExecutions();
        } finally {
            executorService.shutdownNow();
        }
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

    private void printSettings(ApplicationArguments args) {
        final String version = workload.databaseVersion();
        final String isolationLevel = workload.isolationLevel();
        final String driver = DatabaseInfo.driverVersion(dataSource);

        logger.info("Args: %s".formatted(Arrays.stream(args.getSourceArgs()).toList()));

        logger.highlight("Database");
        {
            DatabaseInfo.inspectDatabaseMetadata(dataSource,
                    (k, v) -> logger.info(k + ": %s".formatted(v)));
            logger.info("Database Version: %s".formatted(version));
            logger.info("Driver Version: %s".formatted(driver));
            logger.info("Transaction Isolation: %s".formatted(isolationLevel));
        }

        logger.highlight("Workload Commons");
        {
            logger.info("Workload Type: %s".formatted(settings.getWorkloadType()));
            logger.info("Account Total: %d".formatted(settings.getNumAccounts()));
            logger.info("Account Selection: %d (%.1f%%)"
                    .formatted(settings.getSelection(),
                            (double) settings.getSelection() / (double) settings.getNumAccounts() * 100.0));
            logger.info("Sequential Selection: %s".formatted(!settings.isRandomSelection()));
        }

        logger.highlight("Workload Specifics");
        {
            logger.info("R/W Ratio (P2 only): %s".formatted(settings.getReadWriteRatio()));
            logger.info("Contention Level (P4 only): %s".formatted(settings.getContentionLevel()));
        }

        logger.highlight("Concurrency");
        {
            logger.info("Worker Threads: %d".formatted(settings.getWorkers()));
            logger.info("Iterations: %d".formatted(settings.getIterations()));
            logger.info("Retry Jitter: %s".formatted(settings.isRetryJitter()));
            logger.info("Skip Retries: %s".formatted(settings.isSkipRetry()));
            logger.info("Skip DDL preset: %s".formatted(settings.isSkipCreate()));
            logger.info("Skip DML preset: %s".formatted(settings.isSkipInit()));
        }

        logger.highlight("Safety");
        {
            logger.info("Lock Type: %s".formatted(settings.getLockType()));
            logger.info("Isolation Level: %s".formatted(settings.getIsolationLevel()));
        }
    }

    private void printResults(Duration duration,
                              int commits, int fails, int totalRetries,
                              List<Duration> allDurations) {

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


        logger.highlight("Workload Summary");
        {
            logger.info("Workload: %s".formatted(settings.getWorkloadType()));
            logger.info("Account Total: %d".formatted(settings.getNumAccounts()));
            logger.info("Account Selection: %d (%.1f%%)"
                    .formatted(settings.getSelection(),
                            (double) settings.getSelection() / (double) settings.getNumAccounts() * 100.0));
            logger.info("Threads: %d".formatted(settings.getWorkers()));
            logger.info("Iterations: %d".formatted(settings.getIterations()));
            logger.info("Isolation Level: %s".formatted(settings.getIsolationLevel()));
            logger.info("Lock Type: %s".formatted(settings.getLockType()));
        }

        logger.highlight("Transactions");
        {
            logger.info("Execution Time: %s".formatted(duration));
            logger.info("Total Commits: %,d".formatted(commits));
            logger.info("Total Fails: %,d".formatted(fails));
            logger.info("Total Retries: %,d".formatted(totalRetries));
        }

        logger.highlight("Timings");
        {
            logger.info("Avg time in txn: %.1f ms".formatted(summaryStatistics.getAverage()));
            logger.info("Cumulative time in txn: %.0f ms".formatted(summaryStatistics.getSum()));
            logger.info("Min time in txn: %.1f ms".formatted(summaryStatistics.getMin()));
            logger.info("Max time in txn: %.1f ms".formatted(summaryStatistics.getMax()));
            logger.info("Total samples: %d".formatted(summaryStatistics.getCount()));
            logger.info("P50 latency: %.1f ms".formatted(percentile(allDurationMillis, .50)));
            logger.info("P95 latency: %.1f ms".formatted(percentile(allDurationMillis, .95)));
            logger.info("P99 latency: %.1f ms".formatted(percentile(allDurationMillis, .99)));
            logger.info("P999 latency: %.1f ms".formatted(percentile(allDurationMillis, .999)));
        }

        logger.highlight("Safety");
        {
            final String isolationLevel = workload.isolationLevel();

            logger.info("Lock Type: %s".formatted(settings.getLockType()));
            logger.info("Isolation Level: %s".formatted(settings.getIsolationLevel()));
            logger.info("Reported Isolation Level: %s".formatted(isolationLevel));

            if (!isolationLevel.replace(" ", "_")
                    .equalsIgnoreCase(settings.getIsolationLevel().name())) {
                logger.warn("CAUTION: Configured and reported isolation level differs!!");
            }
        }

        if (fails > 0) {
            logger.error("There are %d non-transient errors that invalidates the final outcome!".formatted(fails));
        }

        if (settings.isExportCsv()) {
            try (Exporter exporter = new CsvExporter(Path.of("chaos.csv"))) {
                exporter.writeHeader(List.of("name", "value", "unit"));
                exporter.write(List.of("duration", duration, "time"));
                exporter.write(List.of("threads", settings.getWorkers(), "counter"));
                exporter.write(List.of("contentionLevel", settings.getContentionLevel(), "counter"));
                exporter.write(List.of("selection", settings.getSelection(), "counter"));
                exporter.write(List.of("accounts", settings.getNumAccounts(), "counter"));
                exporter.write(List.of("commits", commits, "counter"));
                exporter.write(List.of("fails", fails, "counter"));
                exporter.write(List.of("retries", totalRetries, "counter"));
                exporter.write(List.of("avgTime", summaryStatistics.getAverage(), "ms"));
                exporter.write(List.of("cumulativeTime", summaryStatistics.getSum(), "ms"));
                exporter.write(List.of("minTime", summaryStatistics.getMin(), "ms"));
                exporter.write(List.of("maxTime", summaryStatistics.getMax(), "ms"));
                exporter.write(List.of("samples", summaryStatistics.getCount(), "counter"));
                exporter.write(List.of("P50", percentile(allDurationMillis, .50), "ms"));
                exporter.write(List.of("P95", percentile(allDurationMillis, .95), "ms"));
                exporter.write(List.of("P99", percentile(allDurationMillis, .99), "ms"));
                exporter.write(List.of("P999", percentile(allDurationMillis, .999), "ms"));
            } catch (IOException e) {
                logger.error("", e);
            }
        }
    }

}
