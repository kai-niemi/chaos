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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jdbc.JdbcRepositoriesAutoConfiguration;

import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.ConsoleOutput;
import io.roach.chaos.util.CsvExporter;
import io.roach.chaos.util.DatabaseInfo;
import io.roach.chaos.util.Exporter;
import io.roach.chaos.workload.Workload;

import static io.roach.chaos.util.ConsoleOutput.error;
import static io.roach.chaos.util.ConsoleOutput.info;
import static io.roach.chaos.util.ConsoleOutput.printRight;

@SpringBootApplication(exclude = {
        JdbcRepositoriesAutoConfiguration.class
})
public class Application implements ApplicationRunner {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    private DataSource dataSource;

    @Autowired
    private Settings settings;

    @Autowired
    private Workload workload;

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

    @Override
    public void run(ApplicationArguments args) {
        if (settings.getSelection() > settings.getNumAccounts()) {
            logger.warn("Selection (%d) larger than number of accounts (%d)!"
                    .formatted(settings.getSelection(), settings.getNumAccounts()));
            settings.setSelection(settings.getNumAccounts());
        }

        if (settings.getWorkers() <= 0) {
            settings.setWorkers(Runtime.getRuntime().availableProcessors() * 2);
            logger.warn("Setting workers to %d".formatted(settings.getWorkers()));
        }

        workload.validateSettings();

        printSettings(args);

        if (settings.isQuit()) {
            info("Quitting..");
            System.exit(0);
        }

        final ExecutorService executorService = Executors.newFixedThreadPool(settings.getWorkers());
        final Instant startTime = Instant.now();
        final List<Duration> allDurations = new ArrayList<>();
        final AtomicInteger totalRetries = new AtomicInteger();
        int commits = 0;
        int fails = 0;

        try {
            workload.beforeAllExecutions();

            final Deque<Future<List<Duration>>> futures = new ArrayDeque<>();

            // Queue workers
            IntStream.rangeClosed(1, settings.getIterations())
                    .forEach(value -> futures.add(executorService.submit(workload::oneExecution)));

            // Await completion
            while (!futures.isEmpty()) {
                AsciiArt.printProgressBar(settings.getIterations(),
                        settings.getIterations() - futures.size(),
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
        } finally {
            printResults(Duration.between(startTime, Instant.now()), commits, fails, totalRetries.get(), allDurations);
            workload.afterAllExecutions();
            executorService.shutdownNow();
        }
    }

    private void printSettings(ApplicationArguments args) {
        final String version = workload.databaseVersion();
        final String isolationLevel = workload.isolationLevel();
        final String driver = DatabaseInfo.driverVersion(dataSource);

        printRight("Args:", "%s".formatted(Arrays.stream(args.getSourceArgs()).toList()));

        ConsoleOutput.header("Database");
        {
            DatabaseInfo.inspectDatabaseMetadata(dataSource,
                    (k, v) -> printRight(k + ":", "%s".formatted(v)));
            printRight("Database Version:", "%s".formatted(version));
            printRight("Driver Version:", "%s".formatted(driver));
            printRight("Database Isolation:", "%s".formatted(isolationLevel));
        }

        ConsoleOutput.header("Workload Commons");
        {
            printRight("Workload Type:", "%s".formatted(settings.getWorkloadType()));
            printRight("Account Total:", "%d".formatted(settings.getNumAccounts()));
            printRight("Account Selection:", "%d (%.1f%%)"
                    .formatted(settings.getSelection(),
                            (double) settings.getSelection() / (double) settings.getNumAccounts() * 100.0));
            printRight("Sequential Selection:", "%s".formatted(!settings.isRandomSelection()));
        }

        ConsoleOutput.header("Workload Specifics");
        {
            printRight("R/W Ratio (P2 only):", "%s".formatted(settings.getReadWriteRatio()));
            printRight("Contention Level (P4 only):", "%s".formatted(settings.getContentionLevel()));
        }

        ConsoleOutput.header("Concurrency");
        {
            printRight("Worker Threads:", "%d".formatted(settings.getWorkers()));
            printRight("Iterations:", "%d".formatted(settings.getIterations()));
            printRight("Retry Jitter:", "%s".formatted(settings.isRetryJitter()));
            printRight("Skip Retries:", "%s".formatted(settings.isSkipRetry()));
            printRight("Skip DDL preset:", "%s".formatted(settings.isSkipCreate()));
            printRight("Skip DML preset:", "%s".formatted(settings.isSkipInit()));
        }

        ConsoleOutput.header("Safety");
        {
            printRight("Lock Type:", "%s".formatted(settings.getLockType()));
            printRight("Isolation Level:", "%s".formatted(settings.getIsolationLevel()));
            printRight("Isolation Level Reported:", "%s".formatted(isolationLevel));
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

        ConsoleOutput.header("Transactions");
        {
            printRight("Execution time:", "%s".formatted(duration));
            printRight("Total commits:", "%,d".formatted(commits));
            printRight("Total fails:", "%,d".formatted(fails));
            printRight("Total retries:", "%,d".formatted(totalRetries));
        }

        ConsoleOutput.header("Timings");
        {
            printRight("Avg time in txn:", "%.1f ms".formatted(summaryStatistics.getAverage()));
            printRight("Cumulative time in txn:", "%.0f ms".formatted(summaryStatistics.getSum()));
            printRight("Min time in txn:", "%.1f ms".formatted(summaryStatistics.getMin()));
            printRight("Max time in txn:", "%.1f ms".formatted(summaryStatistics.getMax()));
            printRight("Total samples:", "%d".formatted(summaryStatistics.getCount()));
            printRight("P50 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .50)));
            printRight("P95 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .95)));
            printRight("P99 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .99)));
            printRight("P999 latency:", "%.1f ms".formatted(percentile(allDurationMillis, .999)));
        }

        if (fails > 0) {
            error("There were %d non-transient errors that may invalidate the final outcome!".formatted(fails));
        }

        if (settings.isExport()) {
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
