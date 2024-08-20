package io.roach.chaos.repository;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import io.roach.chaos.model.Account;
import io.roach.chaos.model.AccountType;
import io.roach.chaos.util.RandomData;

public class MySQLAccountRepository extends AbstractAccountRepository {
    @Override
    public String isolationLevel() {
        return jdbcTemplate
                .queryForObject("SELECT @@transaction_ISOLATION", String.class);
    }

    @Override
    public List<Account> findRandomAccounts(int limit) {
        return jdbcTemplate.query("SELECT * FROM account ORDER BY rand() LIMIT ?",
                ps -> {
                    ps.setInt(1, limit);
                    ps.setFetchSize(limit);
                }, (rs, rowNum) -> toAccount(rs));
    }

    @Override
    public void createAccounts(BigDecimal initialBalance,
                               int count,
                               Consumer<Integer> progress) {
        jdbcTemplate.execute("TRUNCATE table account");

        for (List<Integer> batch : IntStream.rangeClosed(1, count / 2)
                .boxed()
                .collect(Collectors.groupingBy(it -> it / BATCH_SIZE))
                .values()) {
            jdbcTemplate.batchUpdate("INSERT INTO account(id,balance,name,type)"
                    + " values(?,?,?,?)", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setLong(1, batch.get(i));
                    ps.setBigDecimal(2, initialBalance);
                    ps.setString(3, RandomData.randomString(32));
                    ps.setString(4, AccountType.checking.name());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });

            progress.accept(batch.size());

            jdbcTemplate.batchUpdate("INSERT INTO account(id,balance,name,type)"
                    + " values(?,?,?,?)", new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setLong(1, batch.get(i));
                    ps.setBigDecimal(2, initialBalance);
                    ps.setString(3, RandomData.randomString(32));
                    ps.setString(4, AccountType.credit.name());
                }

                @Override
                public int getBatchSize() {
                    return batch.size();
                }
            });

            progress.accept(batch.size());
        }
    }
}
