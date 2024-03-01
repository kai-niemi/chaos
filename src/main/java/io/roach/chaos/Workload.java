package io.roach.chaos;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

public interface Workload extends Callable<List<Duration>> {
    void beforeExecution(Settings settings, DataSource dataSource, Output output) throws Exception;

    void afterExecution(Output output);
}
