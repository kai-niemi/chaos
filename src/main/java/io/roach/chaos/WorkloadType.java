package io.roach.chaos;

import java.util.function.Supplier;

public enum WorkloadType {
    lost_update(LostUpdate::new) {
        @Override
        public String note() {
            return "P4 lost update. --sfu or --cas required for correct execution in RC.";
        }
    },
    read_skew(ReadSkew::new) {
        @Override
        public String note() {
            return "A5A read skew. --sfu or --cas required for correct execution in RC.";
        }
    },
    write_skew(WriteSkew::new) {
        @Override
        public String note() {
            return "A5B write skew. --cas required for correct execution in RC.";
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
