package io.roach.chaos.model;

public enum LockType {
    NONE {
        @Override
        public String alias() {
            return "NA";
        }
    },
    FOR_UPDATE {
        @Override
        public String alias() {
            return "FU";
        }
    },
    FOR_SHARE {
        @Override
        public String alias() {
            return "FS";
        }
    },
    COMPARE_AND_SET {
        @Override
        public String alias() {
            return "CAS";
        }
    };

    public abstract String alias();
}
