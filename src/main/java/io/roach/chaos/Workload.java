package io.roach.chaos;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

public interface Workload extends Callable<List<Duration>> {
    void setup(Settings settings, DataSource dataSource);

    void beforeExecution(Output output) throws Exception;

    void afterExcution(Output output);
}