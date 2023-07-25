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

package sleeper.systemtest.datageneration;

import java.util.function.LongFunction;

public class RangeValueGenerator implements GenerateRangeByField {

    private final LongFunction<Object> rowKeyGenerator;
    private final LongFunction<Object> sortKeyGenerator;
    private final LongFunction<Object> valueGenerator;

    private RangeValueGenerator(Builder builder) {
        rowKeyGenerator = builder.rowKeyGenerator;
        sortKeyGenerator = builder.sortKeyGenerator;
        valueGenerator = builder.valueGenerator;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Object rowKey(long number) {
        return rowKeyGenerator.apply(number);
    }

    public Object sortKey(long number) {
        return sortKeyGenerator.apply(number);
    }

    public Object value(long number) {
        return valueGenerator.apply(number);
    }

    public static final class Builder {
        private LongFunction<Object> rowKeyGenerator;
        private LongFunction<Object> sortKeyGenerator;
        private LongFunction<Object> valueGenerator;

        private Builder() {
        }

        public Builder rowKeyGenerator(LongFunction<Object> rowKeyGenerator) {
            this.rowKeyGenerator = rowKeyGenerator;
            return this;
        }

        public Builder sortKeyGenerator(LongFunction<Object> sortKeyGenerator) {
            this.sortKeyGenerator = sortKeyGenerator;
            return this;
        }

        public Builder valueGenerator(LongFunction<Object> valueGenerator) {
            this.valueGenerator = valueGenerator;
            return this;
        }

        public RangeValueGenerator build() {
            return new RangeValueGenerator(this);
        }
    }
}
