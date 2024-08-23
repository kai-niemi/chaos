package io.roach.chaos.model;

public enum WorkloadType {
    NON_REPEATABLE_READ {
        @Override
        public String alias() {
            return "P2";
        }
    },
    PHANTOM_READ {
        @Override
        public String alias() {
            return "P3";
        }
    },
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
