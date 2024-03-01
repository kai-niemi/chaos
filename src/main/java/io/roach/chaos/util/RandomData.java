package io.roach.chaos.util;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public abstract class RandomData {
    private static final char[] VOWELS = "aeiou".toCharArray();

    private static final char[] CONSONANTS = "bcdfghjklmnpqrstvwxyz".toCharArray();

    public static <E> E selectRandom(List<E> collection) {
        return collection.get(ThreadLocalRandom.current().nextInt(collection.size()));
    }

    public static String randomString(int min) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder();
        boolean vowelStart = true;
        for (int i = 0; i < min; i++) {
            if (vowelStart) {
                sb.append(VOWELS[(int) (random.nextDouble() * VOWELS.length)]);
            } else {
                sb.append(CONSONANTS[(int) (random.nextDouble() * CONSONANTS.length)]);
            }
            vowelStart = !vowelStart;
        }
        return sb.toString();
    }

}
