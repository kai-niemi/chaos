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

    boolean lock;

    boolean cas;

    boolean jitter;

    boolean readCommitted = false;

    boolean debugProxy = false;

    boolean skipDDL;

    String dialect = "crdb";

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
