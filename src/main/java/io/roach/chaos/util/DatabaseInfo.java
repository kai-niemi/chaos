package io.roach.chaos.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DataSourceUtils;

public abstract class DatabaseInfo {
    private DatabaseInfo() {
    }

    public static String driverVersion(DataSource dataSource) {
        try (Connection connection = DataSourceUtils.doGetConnection(dataSource)) {
            DatabaseMetaData metaData = connection.getMetaData();
            return "%s %s".formatted(metaData.getDriverName(), metaData.getDriverVersion());
        } catch (SQLException e) {
            throw new RuntimeException("Error reading metadata", e);
        }
    }

    public static void inspectDatabaseMetadata(DataSource dataSource,
                                               BiConsumer<String, Object> callback) {

        Map<String, Object> map = new LinkedHashMap<>();

        try (Connection connection = DataSourceUtils.doGetConnection(dataSource)) {
            DatabaseMetaData metaData = connection.getMetaData();

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
        } catch (SQLException e) {
            throw new RuntimeException("Error reading metadata", e);
        }
    }
}
