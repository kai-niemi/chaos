package io.roach.chaos;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import io.roach.chaos.util.AnsiColor;
import io.roach.chaos.util.AsciiArt;

public abstract class AbstractWorkload implements Workload {
    protected Settings settings;

    protected DataSource dataSource;

    @Override
    public void beforeExecution(Settings settings, DataSource dataSource, Output output) throws Exception {
        this.settings = settings;
        this.dataSource = dataSource;

        if (!settings.skipCreate) {
            AccountRepository.createSchema(dataSource, settings.dialect);
        }

        if (!settings.skipInit) {
            AtomicInteger c = new AtomicInteger();
            AccountRepository.createAccounts(dataSource,
                    new BigDecimal("500.00"), settings.numAccounts,
                    v -> System.out.printf("\r%s%s%s",
                            AnsiColor.BOLD_BRIGHT_CYAN.getCode(),
                            AsciiArt.progressBar(settings.numAccounts, c.addAndGet(v),
                                    "Creating %,d accounts".formatted(settings.numAccounts)),
                            AnsiColor.RESET.getCode()));
        }
    }
}
