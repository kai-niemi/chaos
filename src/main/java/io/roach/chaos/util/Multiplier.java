package io.roach.chaos.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Multiplier {
    private static final Pattern DECIMAL_PATTERN
            = Pattern.compile("^[+-]?([0-9]+\\.?[0-9]*|\\.[0-9]+)\\s?([kKmMgG]+)");

    private static final Pattern LONG_PATTERN
            = Pattern.compile("^[+-]?([0-9]+)\\s?([kKmMgG]+)");

    private Multiplier() {
    }

    public static int parseInt(String expression) throws NumberFormatException {
        expression = expression.replace("_", "");
        Matcher matcher = LONG_PATTERN.matcher(expression);
        int value = 0;
        while (matcher.find()) {
            value = Integer.parseInt(matcher.group(1));
            String token = matcher.group(2);
            value = switch (token) {
                case "k", "K" -> 1000 * value;
                case "m", "M" -> 1_000_000 * value;
                case "g", "G" -> 1_000_000_000 * value;
                default -> throw new NumberFormatException("Invalid multiplier " + token);
            };
        }
        if (value == 0) {
            return Integer.parseInt(expression);
        }
        return value;
    }

}
