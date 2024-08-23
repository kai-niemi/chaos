package io.roach.chaos;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.roach.chaos.model.Dialect;
import io.roach.chaos.repository.AccountRepository;
import io.roach.chaos.repository.CockroachAccountRepository;
import io.roach.chaos.repository.MySQLAccountRepository;
import io.roach.chaos.repository.OracleAccountRepository;
import io.roach.chaos.repository.PostgresAccountRepository;
import io.roach.chaos.workload.LostUpdate;
import io.roach.chaos.workload.NonRepeatableRead;
import io.roach.chaos.workload.PhantomRead;
import io.roach.chaos.workload.ReadSkew;
import io.roach.chaos.workload.Workload;
import io.roach.chaos.workload.WriteSkew;

@Configuration
@ConfigurationPropertiesScan(basePackageClasses = Application.class)
public class ApplicationConfig {
    @Autowired
    public Settings settings;

    @Value("${spring.datasource.url}")
    private String url;

    @Bean
    public Workload workload() {
        switch (settings.getWorkloadType()) {
            case NON_REPEATABLE_READ -> {
                return new NonRepeatableRead();
            }
            case PHANTOM_READ -> {
                return new PhantomRead();
            }
            case READ_SKEW -> {
                return new ReadSkew();
            }
            case WRITE_SKEW -> {
                return new WriteSkew();
            }
            case LOST_UPDATE -> {
                return new LostUpdate();
            }
            default -> throw new RuntimeException("Unknown workload: " + settings.getWorkloadType());
        }
    }

    @Bean
    public AccountRepository accountRepository() {
        Dialect dialect = settings.getDialect();

        return switch (dialect) {
            case CRDB -> new CockroachAccountRepository();
            case PSQL -> new PostgresAccountRepository();
            case MYSQL -> new MySQLAccountRepository();
            case ORACLE -> new OracleAccountRepository();
            case NONE -> {
                if (url.startsWith("jdbc:postgresql")
                        || url.startsWith("jdbc:cockroachdb")) {
                    yield new CockroachAccountRepository();
                }
                if (url.startsWith("jdbc:mysql")) {
                    yield new MySQLAccountRepository();
                }
                if (url.startsWith("jdbc:oracle")) {
                    yield new OracleAccountRepository();
                }
                yield new PostgresAccountRepository();
            }
        };
    }
}
