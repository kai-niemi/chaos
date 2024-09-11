package io.roach.chaos.util;

import java.util.Locale;

public abstract class AsciiArt {
    private static final String CUU = "\u001B[A";

    private static final String DL = "\u001B[1M";

    private AsciiArt() {
    }

    public static String happy() {
        return "(ʘ‿ʘ)";
    }

    public static String shrug() {
        return "¯\\_(ツ)_/¯";
    }

    public static String flipTableRoughly() {
        return "(ノಠ益ಠ)ノ彡┻━┻";
    }

    public static void printProgressBar(long total, long current, String label) {
        double p = (current + 0.0) / (Math.max(1, total) + 0.0);
        int ticks = Math.max(0, (int) (30 * p) - 1);
        String bar = "%,9d/%-,9d %5.1f%% [%-30s] %s".formatted(
                current,
                total,
                p * 100.0,
                new String(new char[ticks]).replace('\0', '#') + ">",
                label);
        System.out.println(CUU + "\r" + DL + bar);
        System.out.flush();
    }

    public static void printProgressBar(long total, long current, String label,
                                        double callsPerSec, long remainingMillis) {
        double p = (current + 0.0) / (Math.max(1, total) + 0.0);
        int ticks = Math.max(0, (int) (30 * p) - 1);
        String bar = String.format(
                "%,9d/%-,9d %5.1f%% [%-30s] %,7.0f/s eta %s %s",
                current,
                total,
                p * 100.0,
                new String(new char[ticks]).replace('\0', '#') + ">",
                callsPerSec,
                millisecondsToDisplayString(remainingMillis),
                label);
        System.out.println(CUU + "\r" + DL + bar);
        System.out.flush();
    }

    public static String millisecondsToDisplayString(long timeMillis) {
        double seconds = (timeMillis / 1000.0) % 60;
        int minutes = (int) ((timeMillis / 60000) % 60);
        int hours = (int) ((timeMillis / 3600000));

        StringBuilder sb = new StringBuilder();
        if (hours > 0) {
            sb.append(String.format("%dh ", hours));
        }
        if (hours > 0 || minutes > 0) {
            sb.append(String.format("%dm ", minutes));
        }
        if (hours == 0) {
            sb.append(String.format(Locale.US, "%.1fs", seconds));
        }
        return sb.toString();
    }
}
