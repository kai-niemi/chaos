package io.roach.chaos.repository;

import java.util.List;

import io.roach.chaos.model.Account;
import io.roach.chaos.model.LockType;

public class OracleAccountRepository extends MySQLAccountRepository {
    @Override
    public String databaseVersion() {
        return jdbcTemplate
                .queryForObject("SELECT banner FROM v$version", String.class);
    }

    @Override
    public String isolationLevel() {
        return jdbcTemplate
                .queryForObject("select distinct value from V$SES_OPTIMIZER_ENV where lower(name) like '%isolation%'",
                        String.class);
    }

    @Override
    public List<Account> findTargetAccounts(int limit, boolean random) {
        return jdbcTemplate.query(random
                        ? "SELECT * FROM (select * from account ORDER BY DBMS_RANDOM.RANDOM) where rownum<?"
                        : "SELECT * FROM account order by id where rownum<?",
                ps -> {
                    ps.setInt(1, limit);
                    ps.setFetchSize(limit);
                }, (rs, rowNum) -> toAccount(rs));
    }

    @Override
    public Account findById(Account.Id id, LockType lock) {
        if (lock == LockType.FOR_SHARE) {
            jdbcTemplate.execute("LOCK TABLE account IN SHARE MODE");
        }
        return jdbcTemplate.queryForObject(
                "SELECT * FROM account WHERE id = ? AND type = ?"
                        + (lock == LockType.FOR_UPDATE ? " FOR UPDATE" : ""),
                (rs, rowNum) -> toAccount(rs),
                id.getId(),
                id.getType().name());
    }

}
