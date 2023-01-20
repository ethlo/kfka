package com.ethlo.kfka.util;

/*-
 * #%L
 * kfka-core
 * %%
 * Copyright (C) 2017 - 2021 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
