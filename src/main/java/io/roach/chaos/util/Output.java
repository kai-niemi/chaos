package io.roach.chaos.util;

public interface Output {
    void separator(String title, char character, int width);

    void headerOne(String text);

    void headerTwo(String text);

    void info(String text);

    void error(String text);

    void warn(String text);

    void printLeft(String col1, String col2);

    void printLeft(String col1, String col2, String col3);

    void printRight(String col1, String col2);
}
