package io.roach.chaos.repository;

public class PostgresAccountRepository extends CockroachAccountRepository {
    @Override
    protected String generateSeriesSQL() {
        return "select i FROM generate_series(1, ?) AS i";
    }
}
