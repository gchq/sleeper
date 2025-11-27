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
 * Record for holding relationship between two paired objects.
 *
 * @param <T1>   generic object declaration for first element of pairing
 * @param <T2>   generic object declaration for second element of pairing
 * @param first  first object to pair
 * @param second second object to pair
 */
public record Pair<T1, T2>(T1 first, T2 second) {

    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    public T1 getFirst() {
        return first;
    }

    public T2 getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (first != null && ((Pair) o).first != null) {
            return first.equals(((Pair) o).first);
        }

        if (second != null && ((Pair) o).second != null) {
            return second.equals(((Pair) o).second);
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;

        result = 31 * result + (second != null ? second.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return "NewPair{" + "first=" + first + ", second=" + second + '}';
    }
}
