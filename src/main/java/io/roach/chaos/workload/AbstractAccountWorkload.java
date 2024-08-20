package io.roach.chaos.workload;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import io.roach.chaos.model.Settings;
import io.roach.chaos.repository.AccountRepository;
import io.roach.chaos.util.AsciiArt;
import io.roach.chaos.util.Exporter;
import io.roach.chaos.util.RetryableTransactionWrapper;
import io.roach.chaos.util.TransactionWrapper;

@Component
public abstract class AbstractAccountWorkload implements Workload {
    @Autowired
    protected Settings settings;

    @Autowired
    protected AccountRepository accountRepository;

    @Autowired
    protected DataSource dataSource;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    protected TransactionWrapper transactionWrapper() {
        if (settings.isSkipRetry()) {
            return new TransactionWrapper(platformTransactionManager)
                    .setIsolationLevel(settings.getIsolationLevel());
        }
        return new RetryableTransactionWrapper(platformTransactionManager)
                .setRetryJitter(settings.isRetryJitter())
                .setIsolationLevel(settings.getIsolationLevel());
    }

    @Override
    public String databaseVersion() {
        return accountRepository.databaseVersion();
    }

    @Override
    public String isolationLevel() {
        return accountRepository.isolationLevel();
    }

    @Override
    public final void doBeforeExecution() {
        if (!settings.isSkipCreate()) {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.addScript(new ClassPathResource(settings.getInitFile()));
            populator.setCommentPrefixes("--", "#");
            populator.setIgnoreFailedDrops(true);
            populator.setContinueOnError(true);

            DatabasePopulatorUtils.execute(populator, dataSource);
        }

        if (!settings.isSkipInit()) {
            AtomicInteger c = new AtomicInteger();
            accountRepository.createAccounts(
                    new BigDecimal("500.00"), settings.getNumAccounts(),
                    v -> AsciiArt.printProgressBar(settings.getNumAccounts(), c.addAndGet(v),
                            "Creating %,d accounts".formatted(settings.getNumAccounts())));
        }

        beforeExecution();
    }

    protected void beforeExecution() {
    }

    @Override
    public final void doAfterExecution(Exporter exporter) {
        afterExecution(exporter);
    }

    protected void afterExecution(Exporter exporter) {
    }
}