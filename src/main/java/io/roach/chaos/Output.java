package io.roach.chaos;

public interface Output {
    void debug(String text);

    void header(String text);

    void info(String text);

    void error(String text);
}
