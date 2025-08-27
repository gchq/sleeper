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
package sleeper.core.iterator;

import java.util.Objects;

/**
 * Defines an aggregation operation with the column name to perform on it.
 *
 * @param column the column to aggregate
 * @param op     the aggregation operator
 */
public record Aggregation(String column, AggregationOp op) {

    public Aggregation {
        Objects.requireNonNull(column, "column");
        Objects.requireNonNull(op, "aggregationOp");
    }
}
