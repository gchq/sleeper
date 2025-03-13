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

package sleeper.systemtest.dsl.query;

public class QueryRange {
    private final Object min;
    private final Object max;

    private QueryRange(Object min, Object max) {
        this.min = min;
        this.max = max;
    }

    public static QueryRange range(Object min, Object max) {
        return new QueryRange(min, max);
    }

    public Object getMin() {
        return min;
    }

    public Object getMax() {
        return max;
    }
}
