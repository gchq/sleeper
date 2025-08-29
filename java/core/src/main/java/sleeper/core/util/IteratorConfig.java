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

import sleeper.core.schema.Schema;

/** Config class for getting iterator's from the iterator factory. */
public class IteratorConfig {
    private final String iteratorClassName;
    private final String iteratorConfigString;
    private final String aggregationString;
    private final String filters;
    private final Schema schema;

    public IteratorConfig(Builder builder) {
        this.iteratorClassName = builder.iteratorClassName;
        this.iteratorConfigString = builder.iteratorConfigString;
        this.aggregationString = builder.aggregationString;
        this.filters = builder.filters;
        this.schema = builder.schema;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getIteratorConfigString() {
        return iteratorConfigString;
    }

    public String getAggregationString() {
        return aggregationString;
    }

    public String getFilters() {
        return filters;
    }

    public Schema getSchema() {
        return schema;
    }

    /**
     * Builder for iterator config object.
     */
    public static final class Builder {
        private String iteratorClassName;
        private String iteratorConfigString;
        private String filters;
        private String aggregationString;
        private Schema schema;

        private Builder() {
        }

        /**
         * Sets the iterator class name.
         *
         * @param  iteratorClassName the name of the iterator class to build
         * @return                   builder for method chaining
         */
        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        /**
         * Sets the iterator config string.
         *
         * @param  iteratorConfigString the config string to be used for the iterator
         * @return                      builder for method chaining
         */
        public Builder iteratorConfigString(String iteratorConfigString) {
            this.iteratorConfigString = iteratorConfigString;
            return this;
        }

        /**
         * Sets the filters string.
         *
         * @param  filters the filters string to be used for the iterator
         * @return         builder for method chaining
         */
        public Builder filters(String filters) {
            this.filters = filters;
            return this;
        }

        /**
         * Sets the aggregation string.
         *
         * @param  aggregationString the config string to be used for the aggregation
         * @return                   builder for method chaining
         */
        public Builder aggregationString(String aggregationString) {
            this.aggregationString = aggregationString;
            return this;
        }

        /**
         * Sets the schema for the iterator to use.
         *
         * @param  schema the schema for the iterator to use
         * @return        builder for method chaining
         */
        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public IteratorConfig build() {
            return new IteratorConfig(this);
        }
    }
}
