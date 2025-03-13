/*
 * Copyright 2022-2025 Crown Copyright
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

/**
 * Generates internal IDs for Sleeper tables.
 */
public class TableIdGenerator {

    private final Random random;

    public TableIdGenerator() {
        random = new SecureRandom();
    }

    private TableIdGenerator(int seed) {
        random = new Random(seed);
    }

    /**
     * Creates a generator with a fixed seed for random data generation. This can be used to produce deterministic
     * results. This should only be used for tests.
     *
     * @param  seed the seed for the random generator
     * @return      an instance of this class
     */

    public static TableIdGenerator fromRandomSeed(int seed) {
        return new TableIdGenerator(seed);
    }

    /**
     * Generates a random string.
     *
     * @return a random string
     */
    public String generateString() {
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        return Hex.encodeHexString(bytes);
    }
}
