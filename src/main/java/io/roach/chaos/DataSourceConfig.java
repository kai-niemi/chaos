package io.roach.chaos;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.zaxxer.hikari.HikariDataSource;

import net.ttddyy.dsproxy.listener.logging.DefaultQueryLogEntryCreator;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.listener.logging.SLF4JQueryLoggingListener;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

@Configuration
public class DataSourceConfig {
    private final Logger sqlTraceLogger = LoggerFactory.getLogger("io.roach.SQL_TRACE");

    @Autowired
    private Settings settings;

    @Bean
    @Primary
    public DataSource primaryDataSource() {
        HikariDataSource ds = hikariDataSource();

        DefaultQueryLogEntryCreator creator = new DefaultQueryLogEntryCreator();
        creator.setMultiline(true);

        SLF4JQueryLoggingListener listener = new SLF4JQueryLoggingListener();
        listener.setLogger(sqlTraceLogger);
        listener.setLogLevel(SLF4JLogLevel.TRACE);
        listener.setQueryLogEntryCreator(creator);

        return sqlTraceLogger.isTraceEnabled()
                ? ProxyDataSourceBuilder
                .create(ds)
                .asJson()
                .listener(listener)
                .multiline()
                .build()
                : ds;
    }

    @Bean
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource hikariDataSource() {
        HikariDataSource ds = dataSourceProperties()
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();

        ds.addDataSourceProperty("reWriteBatchedInserts", "true");
        ds.addDataSourceProperty("application_name", "Chaos");
        ds.setTransactionIsolation("TRANSACTION_" + settings.getIsolationLevel().name());
        ds.setAutoCommit(false);
        ds.setInitializationFailTimeout(-1);

        if (ds.getJdbcUrl().startsWith("jdbc:cockroachdb")) {
            ds.setDriverClassName("io.cockroachdb.jdbc.CockroachDriver");
            ds.addDataSourceProperty("implicitSelectForUpdate", "true");
            ds.addDataSourceProperty("retryTransientErrors", "true");
            ds.addDataSourceProperty("retryConnectionErrors", "true");
            ds.addDataSourceProperty("useCockroachMetadata", "true");
        }

        return ds;
    }
}
