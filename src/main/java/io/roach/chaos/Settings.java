package io.roach.chaos;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class Settings {
    String url = "jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable";

    String user = "root";

    String password = "";

    WorkloadType workloadType;

    boolean lock;

    boolean cas;

    boolean jitter;

    boolean export;

    boolean readCommitted = false;

    boolean debugProxy = false;

    boolean skipCreate;

    boolean skipInit;

    boolean skipRetry;

    Dialect dialect = Dialect.crdb;

    int workers = Runtime.getRuntime().availableProcessors() * 2;

    int level = 8;

    int numAccounts = 50_000;

    int selection = 500;

    int iterations = 1000;

    private DataSource dataSource;

    public DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = createDataSource();
        }
        return dataSource;
    }

    private DataSource createDataSource() {
        HikariDataSource hikariDS = new HikariDataSource();
        hikariDS.setJdbcUrl(url);
        hikariDS.setUsername(user);
        hikariDS.setPassword(password);
        hikariDS.setAutoCommit(true);
        hikariDS.setMaximumPoolSize(workers);
        hikariDS.setMinimumIdle(workers);
        hikariDS.setTransactionIsolation(
                readCommitted ? "TRANSACTION_READ_COMMITTED" : "TRANSACTION_SERIALIZABLE");

        if (url.startsWith("jdbc:cockroachdb")) {
            hikariDS.setDriverClassName("io.cockroachdb.jdbc.CockroachDriver");
            hikariDS.addDataSourceProperty("implicitSelectForUpdate", "true");
            hikariDS.addDataSourceProperty("retryTransientErrors", "true");
            hikariDS.addDataSourceProperty("retryConnectionErrors", "true");
            hikariDS.addDataSourceProperty("useCockroachMetadata", "true");
        }

        DefaultQueryLogEntryCreator creator = new DefaultQueryLogEntryCreator();
        creator.setMultiline(true);

        SLF4JQueryLoggingListener listener = new SLF4JQueryLoggingListener();
        listener.setLogger(LoggerFactory.getLogger("io.roach.SQL"));
        listener.setLogLevel(SLF4JLogLevel.TRACE);
        listener.setQueryLogEntryCreator(creator);

        return debugProxy ?
                ProxyDataSourceBuilder
                        .create(hikariDS)
                        .asJson()
                        .listener(listener)
                        .build()
                : hikariDS;
    }
}
