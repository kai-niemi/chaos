package io.roach.chaos.model;

public enum IsolationLevel {
    READ_COMMITTED {
        @Override
        public String alias() {
            return "RC";
        }
    },
    REPEATABLE_READ {
        @Override
        public String alias() {
            return "RR";
        }
    },
    SERIALIZABLE {
        @Override
        public String alias() {
            return "1SR";
        }
    };

    public abstract String alias();
}
