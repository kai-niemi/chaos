package io.roach.chaos.model;

public enum WorkloadType {
    LOST_UPDATE {
        @Override
        public String alias() {
            return "P4";
        }
    },
    READ_SKEW {
        @Override
        public String alias() {
            return "A5A";
        }
    },
    WRITE_SKEW {
        @Override
        public String alias() {
            return "A5B";
        }
    };

    public abstract String alias();
}
