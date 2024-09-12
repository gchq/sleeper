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
package sleeper.clients.testutil;

import com.google.common.io.CharStreams;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientTestUtils {

    private ClientTestUtils() {
    }

    public static String example(String path) throws IOException {
        try (Reader reader = new InputStreamReader(ClientTestUtils.class.getClassLoader().getResourceAsStream(path))) {
            return CharStreams.toString(reader);
        }
    }

    public static String exampleUUID(String start, Object uuidChar) {
        if (start.length() > 8) {
            throw new IllegalArgumentException("Start must be shorter than 8 characters: " + start);
        }
        char character = characterToRepeat(uuidChar);
        return start + IntStream.of(8 - start.length(), 4, 4, 4, 12)
                .mapToObj(size -> uuidPart(size, character))
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
