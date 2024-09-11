package io.roach.chaos.workload;

public enum WorkloadType {
    NON_REPEATABLE_READ {
        @Override
        public String alias() {
            return "P2";
        }

        @Override
        public Workload createInstance() {
            return new NonRepeatableRead();
        }
    },
    PHANTOM_READ {
        @Override
        public String alias() {
            return "P3";
        }

        @Override
        public Workload createInstance() {
            return new PhantomRead();
        }
    },
    LOST_UPDATE {
        @Override
        public String alias() {
            return "P4";
        }

        @Override
        public Workload createInstance() {
            return new LostUpdate();
        }
    },
    READ_SKEW {
        @Override
        public String alias() {
            return "A5A";
        }

        @Override
        public Workload createInstance() {
            return new ReadSkew();
        }
    },
    WRITE_SKEW {
        @Override
        public String alias() {
            return "A5B";
        }

        @Override
        public Workload createInstance() {
            return new WriteSkew();
        }
    };

    public abstract String alias();

    public abstract Workload createInstance();
}
