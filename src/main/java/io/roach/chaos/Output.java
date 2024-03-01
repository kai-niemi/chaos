package io.roach.chaos;

public interface Output {
    void header(String text);
    void headerHighlight(String text);

    void column(String col1, String col2);

    void columnLeft(String col1, String col2);
    void columnLeft(String col1, String col2, String col3);

    void info(String text);

    void error(String text);
}
