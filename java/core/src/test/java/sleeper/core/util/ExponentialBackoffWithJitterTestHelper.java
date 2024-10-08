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
package sleeper.core.util;

import java.util.Random;
import java.util.function.DoubleSupplier;

/**
 * Helpers to create parameters for ExponentialBackoffWithJitter in tests.
 */
public class ExponentialBackoffWithJitterTestHelper {

    private ExponentialBackoffWithJitterTestHelper() {
    }

    /**
     * Creates a fixed seed Random.
     *
     * @return a reference to {@link Random#nextDouble}
     */
    public static DoubleSupplier fixJitterSeed() {
        return new Random(0)::nextDouble;
    }

    /**
     * Creates a double supplier with no jitter.
     *
     * @return a {@link DoubleSupplier} that always returns the same number
     */
    public static DoubleSupplier noJitter() {
        return () -> 1.0;
    }

    /**
     * Creates a double supplier with a fixed jitter fraction.
     *
     * @param  fraction the fraction
     * @return          a {@link DoubleSupplier} that always returns the fraction
     */
    public static DoubleSupplier constantJitterFraction(double fraction) {
        return () -> fraction;
    }
}
