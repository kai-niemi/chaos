package io.roach.chaos;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.roach.chaos.support.DataAccessException;
import io.roach.chaos.support.OptimisticLockException;
import io.roach.chaos.support.Tuple;

public abstract class Repository {
    private Repository() {
    }

    public static Tuple<BigDecimal, Integer> readBalance(Connection conn, long id, boolean lock)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT balance,version FROM account WHERE id = ?"
                        + (lock ? " FOR UPDATE" : ""))) {
            ps.setLong(1, id);

            try (ResultSet res = ps.executeQuery()) {
                if (!res.next()) {
                    throw new IllegalArgumentException("Account not found: " + id);
                }
                return Tuple.of(
                        res.getBigDecimal("balance"),
                        res.getInt("version"));
            }
        }
    }

    public static void updateBalance(
            Connection conn, long id, BigDecimal balance) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE account SET balance = ?, version=version+1 "
                        + "WHERE id = ?")) {
            ps.setBigDecimal(1, balance);
            ps.setLong(2, id);

            if (ps.executeUpdate() != 1) {
                throw new DataAccessException("Rows affected != 1");
            }
        }
    }

    public static void updateBalanceWithCAS(
            Connection conn, long id, BigDecimal balance, int version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE account SET balance = ?, version=version+1 "
                        + "WHERE id = ? AND version=?")) {
            ps.setBigDecimal(1, balance);
            ps.setLong(2, id);
            ps.setInt(3, version);

            if (ps.executeUpdate() != 1) {
                throw new OptimisticLockException("Account id " + id + " version " + version);
            }
        }
    }

    public static BigDecimal readTotalBalance(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "select sum(balance) tot_balance from account where 1=1")) {
            try (ResultSet res = ps.executeQuery()) {
                if (!res.next()) {
                    throw new SQLException("Empty result");
                }
                return res.getBigDecimal("tot_balance");
            }
        }
    }

    public static List<Long> findRandomIDs(Connection conn, int limit) throws SQLException {
        List<Long> ids = new ArrayList<>(limit);

        try (PreparedStatement ps = conn
                .prepareStatement("select id from account order by random() limit ?")) {
            ps.setInt(1, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
        }
        return ids;
    }

}
