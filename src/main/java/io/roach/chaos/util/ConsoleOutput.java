package io.roach.chaos.util;

public abstract class ConsoleOutput {
    private ConsoleOutput() {
    }

    public static void header(String text) {
        print(text, AnsiColor.BOLD_BRIGHT_CYAN);
    }

    public static void info(String text) {
        print(text, AnsiColor.GREEN);
    }

    public static void error(String text) {
        print(text, AnsiColor.BOLD_BRIGHT_RED);
    }

    public static void warn(String text) {
        print(text, AnsiColor.BOLD_BRIGHT_PURPLE);
    }

    public static void print(String text, AnsiColor color) {
        System.out.printf("%s%s%s\n", color.getCode(), text, AnsiColor.RESET.getCode());
    }

    public static void printLeft(String col1, String col2) {
        System.out.printf("%s%-30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
        System.out.printf("%s%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2, AnsiColor.RESET.getCode());
        System.out.println();
    }


    public static void printLeft(String col1, String col2, String col3) {
        System.out.printf("%s%-30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
        System.out.printf("%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2);
        System.out.printf("%s %s%s", AnsiColor.BOLD_BRIGHT_PURPLE.getCode(), col3, AnsiColor.RESET.getCode());
        System.out.println();
    }

    public static void printRight(String col1, String col2) {
        System.out.printf("%s%30s ", AnsiColor.BOLD_BRIGHT_GREEN.getCode(), col1);
        System.out.printf("%s%s%s", AnsiColor.BOLD_BRIGHT_YELLOW.getCode(), col2, AnsiColor.RESET.getCode());
        System.out.println();
    }
}
