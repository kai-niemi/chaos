package io.roach.chaos;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariDataSource;

import io.roach.chaos.workload.Dialect;
import io.roach.chaos.workload.LockType;
import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class Settings {
    public String url = "jdbc:postgresql://localhost:26257/defaultdb?sslmode=disable";

    public String user = "root";

    public String password = "";

    public WorkloadType workloadType;

    public LockType lock = LockType.none;

    public boolean cas;

    public boolean jitter;

    public boolean export;

    public boolean readCommitted = false;

    public boolean debugProxy = false;

    public boolean skipCreate;

    public boolean skipInit;

    public boolean skipRetry;

    public Dialect dialect = Dialect.crdb;

    public int workers = Runtime.getRuntime().availableProcessors() * 2;

    public int level = 8;

    public int numAccounts = 50_000;

    public int selection = 500;

    public int iterations = 1000;

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
