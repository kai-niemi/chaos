package io.roach.chaos;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class Settings {
    //    String url = "jdbc:postgresql://192.168.1.99:26257/test?sslmode=disable";
    String url = "jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable";

    String user = "root";

    String password = "";

    boolean lock;

    boolean cas;

    boolean jitter;

    boolean readCommitted = false;

    boolean debugProxy = false;

    boolean skipCreate = false;

    int workers = Runtime.getRuntime().availableProcessors() * 2;

    int level = 8;

    int numAccounts = 50_000;

    int selection = 500;

    int iterations = 1000;

    public DataSource createDataSource() {
        HikariDataSource hikariDS = new HikariDataSource();
        hikariDS.setJdbcUrl(url);
        hikariDS.setUsername(user);
        hikariDS.setPassword(password);
        hikariDS.setAutoCommit(true);
        hikariDS.setMaximumPoolSize(workers);
        hikariDS.setMinimumIdle(workers);
        hikariDS.setTransactionIsolation(
                readCommitted ? "TRANSACTION_READ_COMMITTED" : "TRANSACTION_SERIALIZABLE");

        return debugProxy ?
                ProxyDataSourceBuilder
                        .create(hikariDS)
                        .asJson()
                        .multiline()
                        .logQueryBySlf4j()
                        .build()
                : hikariDS;
    }
}
