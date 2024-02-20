package io.roach.chaos;

import java.util.function.Supplier;

public enum WorkloadType {
    lost_update(LostUpdateWorkload::new) {
        @Override
        public String note() {
            return "exposed to P4 lost update. Either --sfu or --cas required for correct execution in RC.";
        }
    },
    write_skew(WriteSkewWorkload::new) {
        @Override
        public String note() {
            return "exposed to A5B write skew. --cas required for correct execution in RC.";
        }
    };

    WorkloadType(Supplier<Workload> factory) {
        this.factory = factory;
    }

    private final Supplier<Workload> factory;

    public Supplier<Workload> getFactory() {
        return factory;
    }

    public abstract String note();
}
