package io.roach.chaos;

import java.time.Duration;
import java.time.Instant;

public class Progress {
    private long current;

    private long total;

    private Instant startTime;

    private String label;

    public long getCurrent() {
        return current;
    }

    public Progress setCurrent(long current) {
        this.current = current;
        return this;
    }

    public long getTotal() {
        return total;
    }

    public Progress setTotal(long total) {
        this.total = total;
        return this;
    }

    public Progress setStartTime(Instant startTime) {
        this.startTime = startTime;
        return this;
    }

    public Progress setLabel(String label) {
        this.label = label;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public Duration getElapsedTime() {
        return Duration.between(startTime, Instant.now());
    }

    public double getCallsPerSec() {
        return (double) current / Math.max(1, getElapsedTime().toMillis()) * 1000.0;
    }

    public long getRemainingMillis() {
        double rps = getCallsPerSec();
        return rps > 0 ? (long) ((total - current) / rps * 1000) : 0;
    }
}
