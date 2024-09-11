package io.roach.chaos.repository;

public enum Dialect {
    NONE {
        @Override
        public AccountRepository createInstance(String url) {
            if (url.startsWith("jdbc:postgresql")
                    || url.startsWith("jdbc:cockroachdb")) {
                return new CockroachAccountRepository();
            }
            if (url.startsWith("jdbc:mysql")) {
                return new MySQLAccountRepository();
            }
            if (url.startsWith("jdbc:oracle")) {
                return new OracleAccountRepository();
            }
            return new PostgresAccountRepository();
        }
    },
    CRDB {
        @Override
        public AccountRepository createInstance(String url) {
            return new CockroachAccountRepository();
        }
    },
    PSQL {
        @Override
        public AccountRepository createInstance(String url) {
            return new PostgresAccountRepository();
        }
    },
    ORACLE {
        @Override
        public AccountRepository createInstance(String url) {
            return new OracleAccountRepository();
        }
    },
    MYSQL {
        @Override
        public AccountRepository createInstance(String url) {
            return new MySQLAccountRepository();
        }
    };

    public abstract AccountRepository createInstance(String url);
}
