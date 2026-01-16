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
package sleeper.core.util;

/**
 * A utility to work with percentages. Handles exact integer values.
 */
public class PercentageUtil {

    private PercentageUtil() {
    }

    /**
     * Computes a percentage of a long integer value, rounding up.
     *
     * @param  value   the value
     * @param  percent the percentage out of 100 to compute
     * @return         the result, rounding up
     */
    public static long getCeilPercent(long value, int percent) {
        long multiplied = value * percent;
        if (multiplied % 100 == 0) {
            return multiplied / 100;
        } else {
            return multiplied / 100 + 1;
        }
    }

}
