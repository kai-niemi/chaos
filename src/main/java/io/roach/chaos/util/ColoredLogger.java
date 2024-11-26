package io.roach.chaos.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColoredLogger {
    public static ColoredLogger newInstance() {
        return new ColoredLogger();
    }

    private final Logger ansiLogger = LoggerFactory.getLogger("io.roach.ANSI_ENABLED");

    private final Logger standardLogger = LoggerFactory.getLogger("io.roach.ANSI_DISABLED");

    public void highlight(String format) {
        ansiLogger.info("%s--- %s ---%s".formatted(AnsiColor.BRIGHT_CYAN, format, AnsiColor.RESET));
        standardLogger.info("--- %s ---".formatted(format));
    }

    public void info(String format) {
        int i = format.indexOf(":");
        if (i >= 0) {
            ansiLogger.info("%s%-30s%s%s%s%s".formatted(
                    AnsiColor.BRIGHT_GREEN,
                    format.substring(0, i + 1),
                    AnsiColor.RESET,
                    AnsiColor.BOLD_BRIGHT_YELLOW,
                    format.substring(i + 1),
                    AnsiColor.RESET));
            standardLogger.info("%-30s%s".formatted(
                    format.substring(0, i + 1),
                    format.substring(i + 1)));
        } else {
            ansiLogger.info("%s%s%s".formatted(AnsiColor.GREEN, format, AnsiColor.RESET));
            standardLogger.info(format);
        }
    }

    public void warn(String format) {
        ansiLogger.warn("%s%s%s".formatted(AnsiColor.BRIGHT_YELLOW, format, AnsiColor.RESET));
        standardLogger.warn(format);
    }

    public void error(String format) {
        ansiLogger.info("%s%s%s".formatted(AnsiColor.BRIGHT_RED, format, AnsiColor.RESET));
        standardLogger.error(format);
    }

    public void error(String format, Throwable t) {
        ansiLogger.error(format, t);
        standardLogger.error(format, t);
    }
}
