package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import io.roach.chaos.jdbc.DataAccessException;
import io.roach.chaos.jdbc.JdbcUtils;
import io.roach.chaos.jdbc.OptimisticLockException;
import io.roach.chaos.util.RandomData;

public abstract class AccountRepository {
    public static final int BATCH_SIZE = 512;

    public static void createSchema(DataSource ds, Dialect dialect) {
        JdbcUtils.execute(ds, conn -> {
            StringBuilder buffer = new StringBuilder();

            try {
                URI uri = AccountRepository.class.getResource("/db/create-%s.sql".formatted(dialect)).toURI();
                for (String line : Files.readAllLines(Paths.get(uri))) {
                    if (!line.startsWith("--") && !line.isEmpty()) {
                        buffer.append(line);
                    }
                    if (line.endsWith(";") && !buffer.isEmpty()) {
                        try (Statement statement = conn.createStatement()) {
                            statement.execute(buffer.toString());
                        }
                        buffer.setLength(0);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    public static void createAccounts(DataSource dataSource,
                                      BigDecimal initialBalance,
                                      int count,
                                      Consumer<Integer> progress) {
        JdbcUtils.execute(dataSource, conn -> {
            JdbcUtils.update(conn, "TRUNCATE table account");
            return null;
        });

        Collection<List<Integer>> result = IntStream.rangeClosed(1, count / 2).boxed()
                .collect(Collectors.groupingBy(it -> it / BATCH_SIZE))
                .values();

        for (List<Integer> batch : result) {
            List<Long> generatedIds = JdbcUtils.execute(dataSource, conn -> {
                List<Long> ids = new ArrayList<>(BATCH_SIZE);

                String sql = JdbcUtils.execute(dataSource, JdbcUtils::isCockroachDB)
                        ? "select unordered_unique_rowid() FROM generate_series(1, ?) AS i"
                        : "select nextval('account_seq') FROM generate_series(1, ?) AS i";

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, batch.size());
                    try (ResultSet keys = ps.executeQuery()) {
                        while (keys.next()) {
                            ids.add(keys.getLong(1));
                        }
                    }
                }
                return ids;
            });

            JdbcUtils.execute(dataSource, conn -> {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO account(id,balance,name,type)"
                                + " select"
                                + "  unnest(?) as id,"
                                + "  unnest(?) as balance,"
                                + "  unnest(?) as name,"
                                + "  unnest(?) as type"
                                + " ON CONFLICT (id,type) do nothing")) {
                    List<Long> ids = new ArrayList<>();
                    List<BigDecimal> balances = new ArrayList<>();
                    List<String> names = new ArrayList<>();
                    List<String> types = new ArrayList<>();

                    generatedIds.forEach(id -> {
                        ids.add(id);
                        balances.add(initialBalance);
                        names.add(RandomData.randomString(32));
                        types.add(AccountType.checking.name());

                        ids.add(id);
                        balances.add(initialBalance);
                        names.add(RandomData.randomString(32));
                        types.add(AccountType.credit.name());
                    });

                    ps.setArray(1, ps.getConnection().createArrayOf("LONG", ids.toArray()));
                    ps.setArray(2, ps.getConnection().createArrayOf("DECIMAL", balances.toArray()));
                    ps.setArray(3, ps.getConnection().createArrayOf("VARCHAR", names.toArray()));
                    ps.setArray(4, ps.getConnection().createArrayOf("account_type", types.toArray()));

                    ps.executeLargeUpdate();

                    progress.accept(generatedIds.size() * 2);
                }
                return null;
            });
        }
    }

    public static Account findById(Connection conn, Account.Id id, LockType lock)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM account WHERE id = ? AND type = ?::account_type"
                        + (lock == LockType.for_update ? " FOR UPDATE" :
                        lock == LockType.for_share ? " FOR SHARE" : ""))) {
            ps.setLong(1, id.getId());
            ps.setString(2, id.getType().name());

            try (ResultSet res = ps.executeQuery()) {
                if (!res.next()) {
                    throw new IllegalArgumentException("Account not found: " + id);
                }
                return toAccount(res);
            }
        }
    }

    public static List<Account> findRandomAccounts(Connection conn, int limit) throws SQLException {
        List<Account> accounts = new ArrayList<>(limit);

        try (PreparedStatement ps = conn
                .prepareStatement("SELECT * FROM account " +
                        "ORDER BY random() LIMIT ?")) {
            ps.setInt(1, limit);
            ps.setFetchSize(limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    accounts.add(toAccount(rs));
                }
            }
        }
        return accounts;
    }

    private static Account toAccount(ResultSet res) throws SQLException {
        return new Account()
                .setId(new Account.Id(res.getLong("id"), res.getString("type")))
                .setBalance(res.getBigDecimal("balance"))
                .setVersion(res.getInt("version"));
    }

    public static void updateBalance(Connection conn, Account account) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE account SET balance = ? "
                        + "WHERE id = ? and type=?::account_type")) {
            ps.setBigDecimal(1, account.getBalance());
            ps.setLong(2, account.getId().getId());
            ps.setString(3, account.getId().getType().name());

            if (ps.executeUpdate() != 1) {
                throw new DataAccessException("Rows affected != 1");
            }
        }
    }

    public static void addBalance(Connection connection,
                                  Long id,
                                  AccountType type,
                                  BigDecimal amount)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE account SET balance = balance + ? " +
                        "WHERE id = ? AND type=?::account_type")) {
            ps.setBigDecimal(1, amount);
            ps.setLong(2, id);
            ps.setObject(3, type.name());

            int rows = ps.executeUpdate();
            if (rows != 1) {
                throw new IllegalStateException("Rows affected not 1 but " + rows + " for " + id);
            }
        }
    }

    public static void updateBalanceCAS(Connection conn, Account account) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE account SET balance = ?, version = version + 1 "
                        + "WHERE id = ? AND type=?::account_type AND version=?")) {
            ps.setBigDecimal(1, account.getBalance());
            ps.setLong(2, account.getId().getId());
            ps.setString(3, account.getId().getType().name());
            ps.setInt(4, account.getVersion());

            if (ps.executeUpdate() != 1) {
                throw new OptimisticLockException("" + account);
            }
        }
    }

    public static void addBalanceCAS(Connection connection,
                                     Long id,
                                     AccountType type,
                                     BigDecimal amount,
                                     Integer version)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE account SET balance = balance + ?, version = version + 1 " +
                        "WHERE id = ? AND type=?::account_type AND version=?")) {
            ps.setBigDecimal(1, amount);
            ps.setLong(2, id);
            ps.setObject(3, type.name());
            ps.setInt(4, version);

            if (ps.executeUpdate() != 1) {
                throw new OptimisticLockException("id: " + id + " version: " + version);
            }
        }
    }

    public static BigDecimal sumAccountBalance(Connection connection, Account.Id id)
            throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "select sum(balance) from account where id=?")) {
            ps.setObject(1, id.getId());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1, BigDecimal.class);
                } else {
                    throw new IllegalStateException("No such account  " + id);
                }
            }
        }
    }

    public static BigDecimal sumTotalBalance(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT sum(balance) tot_balance FROM account WHERE 1=1")) {
            try (ResultSet res = ps.executeQuery()) {
                if (!res.next()) {
                    throw new SQLException("Empty result");
                }
                return res.getBigDecimal("tot_balance");
            }
        }
    }
}
