/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.restapi.addTable;

import sleeper.core.properties.table.TableProperties;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A request to add a Sleeper table.
 */
public class AddTableRequest {

    private TableProperties properties;
    private List<Object> splitPoints;

    private AddTableRequest(Builder builder) {
        properties = Objects.requireNonNull(builder.properties, "Request must include 'properties'");
        splitPoints = Optional.ofNullable(builder.splitPoints).orElseGet(List::of);
        Objects.requireNonNull(properties.getSchema(), "Request must include 'schema'");
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Checks that the object created is valid and has all of the required fields.
     *
     * @return the validated object
     */
    public AddTableRequest validate() {
        return AddTableRequest.builder()
                .properties(properties)
                .splitPoints(splitPoints)
                .build();
    }

    public TableProperties getProperties() {
        return properties;
    }

    public List<Object> getSplitPoints() {
        return splitPoints;
    }

    /**
     * Builder to create a request to add a table.
     */
    public static final class Builder {

        private TableProperties properties;
        private List<Object> splitPoints;

        private Builder() {
        }

        /**
         * Sets the table properties, including the schema.
         *
         * @param  properties the properties
         * @return            the builder for chaining
         */
        public Builder properties(TableProperties properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Sets the split points.
         *
         * @param  splitPoints list of split points
         * @return             the builder for chaining
         */
        public Builder splitPoints(List<Object> splitPoints) {
            this.splitPoints = splitPoints;
            return this;
        }

        public AddTableRequest build() {
            return new AddTableRequest(this);
        }
    }
}
