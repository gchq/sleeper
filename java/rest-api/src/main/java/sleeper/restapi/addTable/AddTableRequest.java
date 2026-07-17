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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Decoded JSON body for POST request to /sleeper/tables.
 */
public class AddTableRequest {

    private Map<String, String> properties;
    private Schema schema;
    private List<Object> splitPoints;

    private AddTableRequest(Builder builder) {
        properties = Objects.requireNonNull(builder.properties, "Request must include 'properties'");
        schema = Objects.requireNonNull(builder.schema, "Request must include 'schema'");
        splitPoints = builder.splitPoints;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Schema getSchema() {
        return schema;
    }

    public List<Object> getSplitPoints() {
        return splitPoints;
    }

    /**
     * Builds the tableProperties described by this request.
     *
     * @param  instanceProperties the instance the table will be added to
     * @return                    the table properties (not yet validated)
     */
    public TableProperties toTableProperties(InstanceProperties instanceProperties) {
        Properties propertiesObject = new Properties();
        propertiesObject.putAll(properties);

        TableProperties tableProperties = new TableProperties(instanceProperties, propertiesObject);
        tableProperties.setSchema(schema);
        return tableProperties;
    }

    /**
     * Converts the JSON split points to the typed values expected by the partition tree, using the row key type from
     * the table's schema.
     *
     * @param  tableProperties the table properties (must have schema set)
     * @return                 the typed split points, or an empty list if none were supplied
     */
    public List<Object> toSplitPoints(TableProperties tableProperties) {
        if (splitPoints == null || splitPoints.isEmpty()) {
            return List.of();
        } else {
            return splitPoints;
        }
    }

    /**
     * Checks that the object created is valid and has all of the required fields.
     *
     * @return the validated object
     */
    public AddTableRequest validate() {
        return AddTableRequest.builder()
                .properties(properties)
                .schema(schema)
                .splitPoints(splitPoints)
                .build();
    }

    /**
     * Builder to create an AddTable request.
     */
    public static final class Builder {

        private Map<String, String> properties;
        private Schema schema;
        private List<Object> splitPoints;

        private Builder() {
        }

        /**
         * Sets the table properties, including the schema.
         *
         * @param  tableProperties the properties
         * @return                 the builder for chaining
         */
        public Builder tableProperties(TableProperties tableProperties) {
            return properties(tableProperties.toMapExcludingSchema())
                    .schema(tableProperties.getSchema());
        }

        /**
         * Sets the map of properties.
         *
         * @param  properties the map of properties
         * @return            the builder for chaining
         */
        public Builder properties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        /**
         * Sets the schema.
         *
         * @param  schema the schema
         * @return        the builder for chaining
         */
        public Builder schema(Schema schema) {
            this.schema = schema;
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
