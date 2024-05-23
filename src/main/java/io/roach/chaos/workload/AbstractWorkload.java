package io.roach.chaos.workload;

import io.roach.chaos.Settings;
import io.roach.chaos.util.AsciiArt;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractWorkload implements Workload {
    protected Settings settings;

    @Override
    public void beforeExecution(Output output, Settings settings) throws Exception {
        this.settings = settings;

        DataSource dataSource = settings.getDataSource();

        if (!settings.skipCreate) {
            AccountRepository.createSchema(dataSource, settings.dialect);
        }

        if (!settings.skipInit) {
            AtomicInteger c = new AtomicInteger();
            AccountRepository.createAccounts(dataSource,
                    new BigDecimal("500.00"), settings.numAccounts,
                    v -> AsciiArt.printProgressBar(settings.numAccounts, c.addAndGet(v),
                            "Creating %,d accounts".formatted(settings.numAccounts)));
        }
    }
}
