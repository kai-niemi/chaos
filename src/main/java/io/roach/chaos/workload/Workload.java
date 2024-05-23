package io.roach.chaos.workload;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

import io.roach.chaos.Settings;

public interface Workload extends Callable<List<Duration>> {
    void beforeExecution(Output output, Settings settings) throws Exception;

    void afterExecution(Output output, Exporter exporter) throws Exception;
}
