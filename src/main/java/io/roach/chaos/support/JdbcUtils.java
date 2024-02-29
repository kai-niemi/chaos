package io.roach.chaos.support;

import java.lang.reflect.UndeclaredThrowableException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.sql.DataSource;

public abstract class JdbcUtils {
    private JdbcUtils() {
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

    public static String databaseVersion(Connection connection) throws SQLException {
        return selectOne(connection, "select version()", String.class);
    }

    public static boolean isCockroachDB(Connection connection) throws SQLException {
        return databaseVersion(connection).contains("CockroachDB");
    }

    public static void inspectDatabaseMetadata(Connection connection, BiConsumer<String, Object> callback)
            throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("databaseVersion", databaseVersion(connection));
        map.put("URL", metaData.getURL());
        map.put("databaseProductName", metaData.getDatabaseProductName());
        map.put("databaseMajorVersion", metaData.getDatabaseMajorVersion());
        map.put("databaseMinorVersion", metaData.getDatabaseMinorVersion());
        map.put("databaseProductVersion", metaData.getDatabaseProductVersion());
        map.put("driverMajorVersion", metaData.getDriverMajorVersion());
        map.put("driverMinorVersion", metaData.getDriverMinorVersion());
        map.put("driverName", metaData.getDriverName());
        map.put("driverVersion", metaData.getDriverVersion());
        map.put("maxConnections", metaData.getMaxConnections());
        map.put("defaultTransactionIsolation", metaData.getDefaultTransactionIsolation());
        map.put("transactionIsolation", connection.getTransactionIsolation());

        map.forEach(callback);
    }
}
