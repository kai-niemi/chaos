package io.roach.chaos.workload;

import java.time.Duration;
import java.util.List;

@Note("P3 phantom read anomaly")
public class PhantomRead extends AbstractWorkload {
    @Override
    public List<Duration> doExecute() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
