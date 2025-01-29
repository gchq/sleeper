/*
 * Copyright 2022-2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.core.testutils;

import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Helper functions to create Supplier objects.
 */
public class SupplierTestHelper {

    private SupplierTestHelper() {
    }

    /**
     * Creates a supplier of IDs that would usually be generated with UUID.randomUUID. The IDs will be supplied one at
     * a time from the list, and then throw an exception when there are no more.
     *
     * @param  ids the IDs
     * @return     the supplier
     */
    public static Supplier<String> fixIds(String... ids) {
        return List.of(ids).iterator()::next;
    }

    /**
     * Creates a supplier of IDs that would usually be generated with UUID.randomUUID. The IDs will be generated with
     * each number embedded in the digits of the UUID.
     *
     * @param  prefix the string to start each ID, at most 8 characters
     * @return        the supplier
     */
    public static Supplier<String> supplyNumberedIdsWithPrefix(String prefix) {
        return IntStream.range(1, 10)
                .mapToObj(i -> numberedUUID(prefix, i))
                .iterator()::next;
    }

    /**
     * Creates a supplier that would usually be defined as Instant::now. The supplied times will start at the given
     * time, then each subsequent time will be a minute later than the last.
     *
     * @param  startTime the start time
     * @return           the supplier
     */
    public static Supplier<Instant> timePassesAMinuteAtATimeFrom(Instant startTime) {
        return Stream.iterate(startTime, time -> time.plus(Duration.ofMinutes(1)))
                .iterator()::next;
    }

    /**
     * Creates a supplier that would usually be defined as Instant::now. Will always supply the given time.
     *
     * @param  time the fixed time
     * @return      the supplier
     */
    public static Supplier<Instant> fixTime(Instant time) {
        return () -> time;
    }

    /**
     * Creates a supplier that would usually be defined as Instant::now. Will supply the given times, then error if
     * further times are requested.
     *
     * @param  times the times
     * @return       the supplier
     */
    public static Supplier<Instant> supplyTimes(Instant... times) {
        return List.of(times).iterator()::next;
    }

    /**
     * Creates an example UUID with the first few characters replaced with a string, and the rest set to a given
     * character.
     *
     * @param  start    the string for the start of the UUID, at most 8 characters
     * @param  uuidChar the character to repeat for the rest of the UUID
     * @return          the example UUID as a string
     */
    public static String exampleUUID(String start, Object uuidChar) {
        if (start.length() > 8) {
            throw new IllegalArgumentException("Start must be shorter than 8 characters: " + start);
        }
        char character = characterToRepeat(uuidChar);
        return start + IntStream.of(8 - start.length(), 4, 4, 4, 12)
                .mapToObj(size -> uuidPart(size, character))
                .collect(Collectors.joining("-"));
    }

    /**
     * Creates an example UUID with the first few characters replaced with a string, and the rest set to all zeroes
     * except for a given number at the end.
     *
     * @param  start  the string for the start of the UUID, at most 8 characters
     * @param  number the number to put at the end
     * @return        the example UUID as a string
     */
    public static String numberedUUID(String start, Number number) {
        if (start.length() > 8) {
            throw new IllegalArgumentException("Start must be shorter than 8 characters: " + start);
        }
        int numberInt = number.intValue();
        int remainingCharacters = 32 - start.length();
        Iterator<Character> characters = IntStream.range(0, remainingCharacters)
                .mapToObj(i -> {
                    int exponent = remainingCharacters - i - 1;
                    int digit = numberInt / (int) Math.pow(10, exponent) % 10;
                    return (char) ('0' + digit);
                }).iterator();
        return start + IntStream.of(8 - start.length(), 4, 4, 4, 12)
                .mapToObj(size -> {
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < size; i++) {
                        builder.append(characters.next());
                    }
                    return builder.toString();
                })
                .collect(Collectors.joining("-"));
    }

    private static String uuidPart(int size, char character) {
        return StringUtils.repeat(character, size);
    }

    private static char characterToRepeat(Object charObj) {
        String charStr = charObj.toString();
        if (charStr.length() != 1) {
            throw new IllegalArgumentException("Character to repeat must be a single character: " + charStr);
        }
        return charStr.charAt(0);
    }
}
