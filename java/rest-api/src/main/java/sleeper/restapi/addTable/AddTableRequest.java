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

import com.google.gson.JsonElement;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.local.ReadSplitPoints;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Decoded JSON body for POST request to /sleeper/tables.
 */
public class AddTableRequest {

    private Map<String, String> properties;
    private JsonElement schema;
    private List<JsonElement> splitPoints;

    /**
     * Builds the tableProperties described by this request.
     *
     * @param  instanceProperties the instance the table will be added to
     * @return                    the table properties (not yet validated)
     */
    public TableProperties toTableProperties(InstanceProperties instanceProperties) {
        if (properties == null) {
            throw new IllegalArgumentException("Request must include 'properties'");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Request must include 'schema'");
        }
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setSchema(new SchemaSerDe().fromJson(schema.toString()));
        return tableProperties;
    }

    /**
     * Converts the JSON split points to the typed values expected by the partition tree, using the row key type from
     * the table's schema. Honours {@link TableProperty#SPLIT_POINTS_BASE64_ENCODED} on the supplied table properties.
     *
     * @param  tableProperties the table properties (must have schema set)
     * @return                 the typed split points, or an empty list if none were supplied
     */
    public List<Object> toSplitPoints(TableProperties tableProperties) {
        if (splitPoints == null || splitPoints.isEmpty()) {
            return List.of();
        }
        Schema schema = tableProperties.getSchema();
        if (schema.getRowKeyTypes().size() != 1) {
            throw new IllegalArgumentException(
                    "Split points are only supported for tables with a single row key field");
        }
        boolean stringsBase64Encoded = tableProperties.getBoolean(TableProperty.SPLIT_POINTS_BASE64_ENCODED);
        String joined = splitPoints.stream()
                .map(JsonElement::getAsString)
                .collect(Collectors.joining("\n"));
        return ReadSplitPoints.fromString(joined, schema, stringsBase64Encoded);
    }
}
