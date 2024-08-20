package io.roach.chaos.workload;

import java.time.Duration;
import java.util.List;

import io.roach.chaos.util.Exporter;

public interface Workload {
    String databaseVersion();

    String isolationLevel();

    void doBeforeExecution();

    List<Duration> doExecute();

    void doAfterExecution(Exporter exporter);
}
