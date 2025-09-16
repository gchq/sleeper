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

import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.table.TableProperty.AGGREGATION_CONFIG;
import static sleeper.core.properties.table.TableProperty.FILTERING_CONFIG;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;

/** Configuration for operations to be applied to processed rows with iterators. */
public class IteratorConfig {
    private final String iteratorClassName;
    private final String iteratorConfigString;
    private final String filteringString;
    private final String aggregationString;

    public IteratorConfig(Builder builder) {
        this.iteratorClassName = builder.iteratorClassName;
        this.iteratorConfigString = builder.iteratorConfigString;
        this.filteringString = builder.filteringString;
        this.aggregationString = builder.aggregationString;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates the default iterator configuration for a Sleeper table. This includes the iterators which are applied
     * during compaction.
     *
     * @param  tableProperties the table properties
     * @return                 the configuration
     */
    public static IteratorConfig from(TableProperties tableProperties) {
        return builder()
                .iteratorClassName(tableProperties.get(ITERATOR_CLASS_NAME))
                .iteratorConfigString(tableProperties.get(ITERATOR_CONFIG))
                .filteringString(tableProperties.get(FILTERING_CONFIG))
                .aggregationString(tableProperties.get(AGGREGATION_CONFIG))
                .build();
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getIteratorConfigString() {
        return iteratorConfigString;
    }

    public String getFilteringString() {
        return filteringString;
    }

    public String getAggregationString() {
        return aggregationString;
    }

    /**
     * Builder for iterator config object.
     */
    public static final class Builder {
        private String iteratorClassName;
        private String iteratorConfigString;
        private String filteringString;
        private String aggregationString;

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
         * Sets the filtering configuration.
         *
         * @param  filteringString the filtering configuration to be applied to the data
         * @return                 builder for method chaining
         */
        public Builder filteringString(String filteringString) {
            this.filteringString = filteringString;
            return this;
        }

        /**
         * Sets the aggregation configuration.
         *
         * @param  aggregationString the aggregation configuration to be applied to the data
         * @return                   builder for method chaining
         */
        public Builder aggregationString(String aggregationString) {
            this.aggregationString = aggregationString;
            return this;
        }

        public IteratorConfig build() {
            return new IteratorConfig(this);
        }
    }
}
