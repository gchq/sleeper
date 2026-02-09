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
package sleeper.trino.remotesleeperconnection;

import sleeper.core.key.Key;

import static java.util.Objects.requireNonNull;

/**
 * Holds a pair of Sleeper keys for the min and max of a range. This does not currently handle inclusive/exclusive or
 * unbounded ranges.
 * <p>
 * It may be sensible to discard this class and use a {@link io.trino.spi.predicate.Range} instead, as this holds the
 * bounds for a single column (and this connector does not support rowkeys with multiple columns). It would also avoid
 * having to expose a Sleeper concept ({@link Key}) outside of this package.
 */
public class SleeperKeyRange {
    private final Key min;
    private final Key max;

    public SleeperKeyRange(Key min, Key max) {
        this.min = requireNonNull(min);
        this.max = requireNonNull(max);
    }

    public Key getMin() {
        return min;
    }

    public Key getMax() {
        return max;
    }

    @Override
    public String toString() {
        return "KeyRange{" +
                "min=" + min +
                ", max=" + max +
                '}';
    }
}
