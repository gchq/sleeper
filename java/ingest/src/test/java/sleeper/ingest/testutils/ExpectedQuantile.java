/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.testutils;

import java.util.Objects;

public class ExpectedQuantile<T> {
    private final double index;
    private final T value;

    private ExpectedQuantile(double index, T value) {
        this.index = index;
        this.value = value;
    }

    public static <T> ExpectedQuantile<T> with(double index, T value) {
        return new ExpectedQuantile<T>(index, value);
    }

    public double getIndex() {
        return index;
    }

    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExpectedQuantile<?> expectedQuantile = (ExpectedQuantile<?>) o;
        return index == expectedQuantile.index && Objects.equals(value, expectedQuantile.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, value);
    }
}
