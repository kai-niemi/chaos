package io.roach.chaos.support;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

public abstract class JdbcUtils {
    private JdbcUtils() {
    }

    public static int update(Connection conn, String sql) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            return ps.executeUpdate();
        }
    }

    public static <T> T selectOne(Connection conn, String query, Class<T> type) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getObject(1, type);
            }
            return null;
        }
    }

    public static <T> T execute(DataSource ds, ConnectionCallback<T> action) {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(true);

            T result;
            try {
                result = action.doInConnection(conn);
            } catch (RuntimeException | Error ex) {
                throw ex;
            } catch (Throwable ex) {
                throw new UndeclaredThrowableException(ex,
                        "TransactionCallback threw undeclared checked exception");
            }
            return result;
        } catch (SQLException e) {
            throw new DataAccessException(e);
        }
    }
}
