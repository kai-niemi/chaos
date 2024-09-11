package io.roach.chaos;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.roach.chaos.repository.AccountRepository;
import io.roach.chaos.workload.Workload;

@Configuration
@ConfigurationPropertiesScan(basePackageClasses = Application.class)
public class ApplicationConfig {
    @Autowired
    private Settings settings;

    @Value("${spring.datasource.url}")
    private String url;

    @Bean
    public Workload workload() {
        return settings.getWorkloadType().createInstance();
    }

    @Bean
    public AccountRepository accountRepository() {
        return settings.getDialect().createInstance(url);
    }
}
