package io.roach.chaos;

import java.util.function.Supplier;

public enum WorkloadType {
    lost_update(LostUpdateWorkload::new),
    write_skew(WriteSkewWorkload::new);

    WorkloadType(Supplier<Workload> factory) {
        this.factory = factory;
    }

    private Supplier<Workload> factory;

    public Supplier<Workload> getFactory() {
        return factory;
    }
}
