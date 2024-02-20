package io.roach.chaos;

import javax.sql.DataSource;

public abstract class AbstractWorkload implements Workload {
    protected Settings settings;

    protected DataSource dataSource;

    @Override
    public void setup(Settings settings, DataSource dataSource) {
        this.settings = settings;
        this.dataSource = dataSource;
    }
}
