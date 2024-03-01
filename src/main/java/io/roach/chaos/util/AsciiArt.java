package io.roach.chaos.util;

public abstract class AsciiArt {
    private AsciiArt() {
    }

    public static String happy() {
        return "(ʘ‿ʘ)";
    }

    public static String shrug() {
        return "¯\\_(ツ)_/¯";
    }

    public static String flipTableGently() {
        return "(╯°□°)╯︵ ┻━┻";
    }

    public static String flipTableRoughly() {
        return "(ノಠ益ಠ)ノ彡┻━┻";
    }

    public static String progressBar(int total, int current, String label) {
        double p = (current + 0.0) / (Math.max(1, total) + 0.0);
        int ticks = Math.max(0, (int) (30 * p) - 1);
        return String.format(
                "%,8d/%,-8d %5.1f%%[%-30s] %s",
                current,
                total,
                p * 100.0,
                new String(new char[ticks]).replace('\0', '#') + ">",
                label);
    }
}
