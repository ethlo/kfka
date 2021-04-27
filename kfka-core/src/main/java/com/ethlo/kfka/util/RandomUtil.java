package com.ethlo.kfka.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RandomUtil
{
    private static final String LETTERS = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private RandomUtil()
    {
    }

    public static String generateAsciiString(int length)
    {
        final Random random = ThreadLocalRandom.current();

        return random.ints(0, LETTERS.length())
                .map(LETTERS::charAt)
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
