package io.roach.chaos.workload;

import java.time.Duration;
import java.util.List;

public interface Workload {
    String databaseVersion();

    String isolationLevel();

    default void validateSettings() {
    }

    default void beforeAllExecutions() {
    }

    List<Duration> oneExecution();

    default void afterAllExecutions() {

    }
}
