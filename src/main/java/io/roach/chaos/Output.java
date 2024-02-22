package io.roach.chaos;

public interface Output {
    void pair(String prefix, String suffix);

    void header(String text);

    void info(String text);

    void error(String text);
}
