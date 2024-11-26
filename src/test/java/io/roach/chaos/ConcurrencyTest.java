package io.roach.chaos;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class ConcurrencyTest {
    @Test
    public void givenDecomposedTask_whenRunningConcurrenlyUsingForkJoin_thenMeasureDurations() {
        List<Callable<Duration>> tasks = new ArrayList<>();

        // Create a few tasks that do some imaginary stuff
        IntStream.range(1, 10).forEach(value -> {
            Callable<Duration> task = () -> {
                Instant start = Instant.now();

                // Synthetic delay
                TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(500, 1500));

                // Synthetic fail (with 30% chance)
                if (ThreadLocalRandom.current().nextDouble(1.0) > .7) {
                    throw new IllegalStateException("This task failed successfully!");
                }

                return Duration.between(start, Instant.now());
            };

            tasks.add(task);
        });

        List<Duration> allDurations = Collections.synchronizedList(new ArrayList<>());

        final Instant start = Instant.now();

        // Schedule tasks to run concurrently and wait completion
        runConcurrentlyAndWait(tasks, duration -> {
            System.out.printf("Task thread [%s] completed in %s\n", Thread.currentThread().getName(), duration);
            allDurations.add(duration);
        }, throwable -> {
            System.out.printf("Task thread [%s] failed: %s\n", Thread.currentThread().getName(),
                    throwable.getMessage());
            return null;
        });

        DoubleSummaryStatistics statistics = allDurations
                .stream()
                .mapToDouble(Duration::toMillis)
                .sorted()
                .summaryStatistics();
        System.out.printf("Average duration (ms): %s\n", statistics.getAverage());
        System.out.printf("Min duration (ms): %s\n", statistics.getMin());
        System.out.printf("Max duration (ms): %s\n", statistics.getMax());
        System.out.printf("Sum duration (ms): %s\n", statistics.getSum());
        System.out.printf("Cumulative duration (sum): %s\n",
                allDurations.stream().reduce(Duration.ZERO, Duration::plus));
        System.out.printf("Total duration: %s\n", Duration.between(start, Instant.now()));
    }

    public static <V> void runConcurrentlyAndWait(List<Callable<V>> tasks,
                                                  Consumer<V> completionFunction,
                                                  Function<Throwable, ? extends Void> throwableFunction) {
        List<CompletableFuture<Void>> allFutures = new ArrayList<>();

        tasks.forEach(callable -> allFutures
                .add(CompletableFuture.supplyAsync(() -> {
                                    try {
                                        return callable.call();
                                    } catch (Exception e) {
                                        throw new IllegalStateException(e);
                                    }
                                })
                                .thenAccept(completionFunction)
                                .exceptionally(throwableFunction)
                ));

        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[] {})).join();
    }
}
