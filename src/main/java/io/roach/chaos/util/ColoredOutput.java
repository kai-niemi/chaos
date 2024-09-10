package io.roach.chaos.util;

import java.io.PrintStream;

public class ColoredOutput implements Output {
    private final PrintStream out;

    public ColoredOutput() {
        this.out = System.out;
    }

    @Override
    public void separator(String title, char character, int width) {
        String sep = new String(new char[width / 2 - title.length() - 2])
                .replace('\0', character);
        out.printf("%s%s %s %s%s\n",
                AnsiColor.BOLD_BRIGHT_WHITE.getCode(),
                sep,
                title,
                sep,
                AnsiColor.RESET.getCode());
    }

    @Override
    public void headerOne(String text) {
        header(text, AnsiColor.BOLD_BRIGHT_CYAN);
    }

    @Override
    public void headerTwo(String text) {
        header(text, AnsiColor.BOLD_BRIGHT_PURPLE);
    }

    public void header(String text, AnsiColor color) {
        print(text, color);
    }

    @Override
    public void info(String text) {
        print(text, AnsiColor.GREEN);
    }

    @Override
    public void error(String text) {
        print(text, AnsiColor.BOLD_BRIGHT_RED);
    }

    @Override
    public void warn(String text) {
        print(text, AnsiColor.BOLD_BRIGHT_PURPLE);
    }

    public void print(String text, AnsiColor color) {
        out.printf("%s%s%s\n", color.getCode(), text, AnsiColor.RESET.getCode());
    }

    @Override
    public void printLeft(String col1, String col2) {
        out.printf("%s%-30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
        out.printf("%s%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2, AnsiColor.RESET.getCode());
        out.println();
    }

    @Override
    public void printLeft(String col1, String col2, String col3) {
        out.printf("%s%-30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
        out.printf("%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2);
        out.printf("%s %s%s", AnsiColor.BOLD_BRIGHT_PURPLE.getCode(), col3, AnsiColor.RESET.getCode());
        out.println();
    }

    @Override
    public void printRight(String col1, String col2) {
        out.printf("%s%30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
        out.printf("%s%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2, AnsiColor.RESET.getCode());
        out.println();
    }
}