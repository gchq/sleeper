/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.core.table;

import org.apache.commons.codec.binary.Hex;

import java.security.SecureRandom;
import java.util.Random;

public class TableIdGenerator {

    private final Random random;

    public TableIdGenerator() {
        random = new SecureRandom();
    }

    private TableIdGenerator(int seed) {
        random = new Random(seed);
    }

    public static TableIdGenerator fromRandomSeed(int seed) {
        return new TableIdGenerator(seed);
    }

    public String generateString() {
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        return Hex.encodeHexString(bytes);
    }
}
