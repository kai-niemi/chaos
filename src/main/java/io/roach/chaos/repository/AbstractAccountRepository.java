package io.roach.chaos.repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.JdbcTemplate;

import io.roach.chaos.Settings;
import io.roach.chaos.model.Account;
import io.roach.chaos.model.AccountType;
import io.roach.chaos.model.LockType;

public abstract class AbstractAccountRepository implements AccountRepository {
    public static final int BATCH_SIZE = 512;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected Settings settings;

    @Override
    public String databaseVersion() {
        return jdbcTemplate
                .queryForObject("SELECT version()", String.class);
    }

    @Override
    public String isolationLevel() {
        return jdbcTemplate
                .queryForObject("SHOW transaction_isolation", String.class);
    }


    @Override
    public void createAccount(Account account) {
        jdbcTemplate.update("INSERT INTO account(id,type,balance,name)"
                + " values(?,?,?,?)", ps -> {
            ps.setLong(1, account.getId().getId());
            ps.setString(2, account.getId().getType());
            ps.setBigDecimal(3, account.getBalance());
            ps.setString(4, account.getName());
        });
    }

    @Override
    public void deleteAccount(Account.Id id) {
        jdbcTemplate.update("delete from account where id=? and type=?", ps -> {
            ps.setLong(1, id.getId());
            ps.setString(2, id.getType());
        });
    }

    @Override
    public Account findAccountById(Account.Id id, LockType lock) {
        return jdbcTemplate.queryForObject(
                "SELECT * FROM account WHERE id = ? AND type = ?"
                        + (lock == LockType.FOR_UPDATE ? " FOR UPDATE" :
                        lock == LockType.FOR_SHARE ? " FOR SHARE" : ""),
                (rs, rowNum) -> toAccount(rs),
                id.getId(),
                id.getType());
    }

    @Override
    public List<Account> findAccountsById(Long id, LockType lock) {
        return jdbcTemplate.query(
                "SELECT * FROM account WHERE id=?"
                        + (lock == LockType.FOR_UPDATE ? " FOR UPDATE" :
                        lock == LockType.FOR_SHARE ? " FOR SHARE" : ""),
                ps -> {
                    ps.setLong(1, id);
                }, (rs, rowNum) -> toAccount(rs));
    }

    @Override
    public List<Account> findTargetAccounts(int limit, boolean random) {
        return jdbcTemplate.query(
                random ? "SELECT * FROM account ORDER BY random() LIMIT ?"
                        : "SELECT * FROM account ORDER BY id LIMIT ?",
                ps -> {
                    ps.setInt(1, limit);
                    ps.setFetchSize(limit);
                }, (rs, rowNum) -> toAccount(rs));
    }

    protected Account toAccount(ResultSet res) throws SQLException {
        return new Account()
                .setId(new Account.Id(
                        res.getLong("id"),
                        res.getString("type")))
                .setBalance(res.getBigDecimal("balance"))
                .setVersion(res.getInt("version"));
    }

    @Override
    public void updateBalance(Account account) {
        int rowsUpdated = jdbcTemplate.update(
                "UPDATE account SET balance = ? "
                        + "WHERE id = ? and type=?", ps -> {
                    ps.setBigDecimal(1, account.getBalance());
                    ps.setLong(2, account.getId().getId());
                    ps.setString(3, account.getId().getType());
                });

        if (rowsUpdated != 1) {
            throw new IllegalStateException("Rows affected != 1");
        }
    }

    @Override
    public void updateBalanceCAS(Account account) {
        int rowsUpdated = jdbcTemplate.update(
                "UPDATE account SET balance = ?, version = version + 1 "
                        + "WHERE id = ? AND type=? AND version=?", ps -> {
                    ps.setBigDecimal(1, account.getBalance());
                    ps.setLong(2, account.getId().getId());
                    ps.setString(3, account.getId().getType());
                    ps.setInt(4, account.getVersion());
                });

        if (rowsUpdated != 1) {
            throw new OptimisticLockingFailureException("Rows affected not 1 but " + rowsUpdated + " for " + account);
        }
    }

    @Override
    public void addBalance(Long id,
                           AccountType type,
                           BigDecimal amount) {
        int rowsUpdated = jdbcTemplate.update(
                "UPDATE account SET balance = balance + ? " +
                        "WHERE id = ? AND type=?", ps -> {
                    ps.setBigDecimal(1, amount);
                    ps.setLong(2, id);
                    ps.setString(3, type.name());
                });

        if (rowsUpdated != 1) {
            throw new IllegalStateException("Rows affected not 1 but " + rowsUpdated + " for " + id);
        }
    }

    @Override
    public void addBalanceCAS(Long id,
                              AccountType type,
                              BigDecimal amount,
                              Integer version) {
        int rowsUpdated = jdbcTemplate.update(
                "UPDATE account SET balance = balance + ?, version = version + 1 " +
                        "WHERE id = ? AND type=? AND version=?", ps -> {
                    ps.setBigDecimal(1, amount);
                    ps.setLong(2, id);
                    ps.setString(3, type.name());
                    ps.setInt(4, version);
                });

        if (rowsUpdated != 1) {
            throw new OptimisticLockingFailureException("id: " + id
                    + " type: " + type
                    + " amount: " + amount
                    + " version: " + version);
        }
    }

    @Override
    public BigDecimal totalAccountBalance(Long id) {
        return this.jdbcTemplate.queryForObject(
                "select sum(balance) from account where id=?",
                (rs, rowNum) -> rs.getBigDecimal(1),
                id
        );
    }

    @Override
    public void findNegativeBalances(Consumer<Pair<Long, BigDecimal>> consumer) {
        this.jdbcTemplate.query(
                "select id, sum(balance) total from account group by id",
                rs -> {
                    Long id = rs.getLong(1);
                    BigDecimal balance = rs.getBigDecimal(2);
                    if (balance.compareTo(BigDecimal.ZERO) < 0) {
                        consumer.accept(Pair.of(id, balance));
                    }
                }
        );
    }

    @Override
    public BigDecimal sumTotalBalance() {
        return jdbcTemplate.queryForObject("select sum(balance) from account WHERE 1=1",
                BigDecimal.class);
    }
}
