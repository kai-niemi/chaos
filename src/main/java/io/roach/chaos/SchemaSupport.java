package io.roach.chaos;

import io.roach.chaos.Repository;
import io.roach.chaos.support.ConnectionTemplate;

import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

public abstract class SchemaSupport {
    private SchemaSupport() {
    }

    public static void setupSchema(DataSource ds) throws Exception {
        URL sql = Repository.class.getResource("/db/create.sql");

        StringBuilder buffer = new StringBuilder();

        Files.readAllLines(Paths.get(sql.toURI())).forEach(line -> {
            if (!line.startsWith("--") && !line.isEmpty()) {
                buffer.append(line);
            }
            if (line.endsWith(";") && buffer.length() > 0) {
                ConnectionTemplate.execute(ds, conn -> {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(buffer.toString());
                    }
                    buffer.setLength(0);
                    return null;
                });
            }
        });
    }

    private static final int BATCH_SIZE = 512;

    public static void createAccounts(Connection conn,
                                      BigDecimal initialBalance,
                                      int count) throws SQLException {
        Collection<List<Integer>> result = IntStream.rangeClosed(1, count).boxed()
                .collect(Collectors.groupingBy(it -> it / BATCH_SIZE))
                .values();

        for (List<Integer> integers : result) {
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO account (balance, name) " +
                            "SELECT ?, concat('system:', (i::varchar)) " +
                            "FROM generate_series(1, ?) AS i", Statement.NO_GENERATED_KEYS)) {
                ps.setBigDecimal(1, initialBalance);
                ps.setLong(2, integers.size());
                ps.executeLargeUpdate();
            }
        }
    }

    public static void deleteAccounts(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "TRUNCATE table account")) {
            ps.executeUpdate();
        }
    }

    public static String showIsolationLevel(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn
                .prepareStatement("SHOW transaction_isolation");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        }
    }
}
