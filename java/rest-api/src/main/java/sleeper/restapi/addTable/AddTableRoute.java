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

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.google.gson.JsonSyntaxException;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MapSchema;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.AddTable;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.restapi.Route;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * REST route for adding a table to a Sleeper instance.
 */
public class AddTableRoute implements Route {

    public static final Logger LOGGER = LoggerFactory.getLogger(AddTableRoute.class);
    private final InstanceProperties instanceProperties;
    private final AddTable addTable;
    private final AddTableRequestSerDe requestSerDe;
    private final AddTableResponseSerDe responseSerDe;

    private AddTableRoute(Builder builder) {
        // Fields may be null when this route is instantiated by the OpenAPI doc generator, which only invokes the
        // openApi* methods below. handle() will fail with an NPE if the route is dispatched a request without them.
        instanceProperties = builder.instanceProperties;
        addTable = builder.addTable;
        requestSerDe = new AddTableRequestSerDe();
        responseSerDe = new AddTableResponseSerDe();
    }

    /**
     * Handles a request matched to the addTableRoute. Includes verifying that the request is valid.
     *
     * @param  event request object containing details to perform an add table action
     * @return       an API Gateway response detailing whether the addTable action has suceeded or not
     */
    @Override
    public APIGatewayV2HTTPResponse handle(APIGatewayV2HTTPEvent event) {
        AddTableRequest request;
        try {
            request = requestSerDe.fromJson(Route.decodeBody(event));
        } catch (JsonSyntaxException e) {
            LOGGER.warn("Add table request body was not valid JSON", e);
            return Route.errorResponse(400, "invalid_request", "Request body is not valid JSON");
        }

        if (request == null) {
            return Route.errorResponse(400, "invalid_request", "Request body is empty");
        }

        TableProperties tableProperties;
        List<Object> splitPoints;

        try {
            tableProperties = request.toTableProperties(instanceProperties);
            splitPoints = request.toSplitPoints(tableProperties);
        } catch (RuntimeException e) {
            // SchemaSerDe / split-point parsing surface invalid input as runtime exceptions.
            LOGGER.warn("Add table request was invalid", e);
            return Route.errorResponse(400, "invalid_request", e.getMessage());
        }

        try {
            addTable.addTable(tableProperties, splitPoints);
        } catch (TableAlreadyExistsException e) {
            LOGGER.info("Add table request rejected: table already exists", e);
            return Route.errorResponse(409, "table_already_exists", e.getMessage());
        } catch (IllegalArgumentException e) {
            // SleeperPropertiesInvalidException extends IllegalArgumentException.
            LOGGER.warn("Add table request rejected: properties failed validation", e);
            return Route.errorResponse(400, "invalid_request", e.getMessage());
        }

        AddTableResponse response = AddTableResponse.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .build();

        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(201)
                .withHeaders(Map.of("Content-Type", CONTENT_TYPE_JSON))
                .withBody(responseSerDe.toJson(response))
                .build();
    }

    @Override
    public PathItem.HttpMethod openApiMethod() {
        return PathItem.HttpMethod.POST;
    }

    @Override
    public String openApiPath() {
        return "/sleeper/tables";
    }

    @Override
    public Operation openApiOperation() {
        Map<String, Object> requestExample = linkedMap(
                "properties", linkedMap("sleeper.table.name", "my-table"),
                "schema", linkedMap(
                        "rowKeyFields", List.of(linkedMap("name", "key", "type", "StringType")),
                        "sortKeyFields", List.of(linkedMap("name", "timestamp", "type", "LongType")),
                        "valueFields", List.of(linkedMap("name", "value", "type", "StringType"))),
                "splitPoints", List.of("m"));
        return new Operation()
                .summary("Add a table")
                .description("Creates a new table in the Sleeper instance. Equivalent to running the `addTable.sh` " +
                        "script. See add-table.md for a worked example.")
                .operationId("addTable")
                .requestBody(new RequestBody()
                        .required(true)
                        .content(new Content().addMediaType("application/json", new MediaType()
                                .schema(new Schema<>().$ref("#/components/schemas/AddTableRequest"))
                                .example(requestExample))))
                .responses(new ApiResponses()
                        .addApiResponse("201", jsonResponse(
                                "Table created.",
                                "#/components/schemas/AddTableResponse",
                                linkedMap("tableId", "01HXYZABCDEFGHJKMNPQRSTVWX", "tableName", "my-table")))
                        .addApiResponse("400", jsonResponse(
                                "Request was rejected before the table was created. Triggered by a body that is not " +
                                        "valid JSON, an empty body, missing `properties` or `schema`, unparseable " +
                                        "split points, or property values that fail validation.",
                                "#/components/schemas/Error",
                                linkedMap("error", "invalid_request", "message", "Request must include 'schema'")))
                        .addApiResponse("409", jsonResponse(
                                "A table with the requested name already exists.",
                                "#/components/schemas/Error",
                                linkedMap("error", "table_already_exists",
                                        "message", "Table with name 'my-table' already exists")))
                        .addApiResponse("500", jsonResponse(
                                "The request failed unexpectedly. Consult the REST API Lambda log group.",
                                "#/components/schemas/Error",
                                linkedMap("error", "internal_error", "message", "Failed to action event"))));
    }

    @Override
    public Map<String, Schema<?>> openApiSchemas() {
        Schema<?> propertiesMap = new MapSchema()
                .additionalProperties(new StringSchema())
                .description("Table properties as a flat map of string keys to string values. Keys are Sleeper " +
                        "table property names (see the table properties documentation). The `sleeper.table.name` " +
                        "property is required and determines the name of the new table.")
                .example(linkedMap("sleeper.table.name", "my-table"));

        // Uses Schema<>() with setType("object") rather than ObjectSchema — the latter's setExample falls back to
        // toString() for Map values in swagger-models 2.2.x, which renders a Java map literal in the YAML instead of
        // a nested object.
        Schema<Object> schemaField = new Schema<>();
        schemaField.setType("object");
        schemaField.setDescription("The Sleeper schema for the new table, as JSON. Passed through to `SchemaSerDe` on the " +
                "server, so any shape accepted by the schema template is accepted here. See the schema " +
                "documentation at ../usage/schema.md for the shape and supported field types.");
        schemaField.setExample(linkedMap(
                "rowKeyFields", List.of(linkedMap("name", "key", "type", "StringType")),
                "valueFields", List.of(linkedMap("name", "value", "type", "StringType"))));

        Schema<?> splitPointsList = new ArraySchema()
                .items(new StringSchema())
                .description("Optional pre-split points for the table's partition tree. Each entry is the string " +
                        "representation of a value in the table's single row key column; supplying split points " +
                        "for a multi-row-key table is rejected with a 400.")
                .example(List.of("m"));

        Schema<?> addTableRequest = new ObjectSchema()
                .required(List.of("properties", "schema"))
                .addProperty("properties", propertiesMap)
                .addProperty("schema", schemaField)
                .addProperty("splitPoints", splitPointsList);

        Schema<?> addTableResponse = new ObjectSchema()
                .required(List.of("tableId", "tableName"))
                .addProperty("tableId", new StringSchema()
                        .description("The unique id assigned to the new table."))
                .addProperty("tableName", new StringSchema()
                        .description("The name of the new table (echoed from the request)."));

        LinkedHashMap<String, Schema<?>> schemas = new LinkedHashMap<>();
        schemas.put("AddTableRequest", addTableRequest);
        schemas.put("AddTableResponse", addTableResponse);
        return schemas;
    }

    /**
     * Builds an insertion-ordered map from alternating key/value arguments. Used so the generated OpenAPI spec has
     * stable field ordering across runs — {@link Map#of(Object, Object)} randomises iteration order per JVM.
     */
    private static LinkedHashMap<String, Object> linkedMap(Object... kvPairs) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < kvPairs.length; i += 2) {
            map.put((String) kvPairs[i], kvPairs[i + 1]);
        }
        return map;
    }

    private static ApiResponse jsonResponse(String description, String schemaRef, Object example) {
        return new ApiResponse()
                .description(description)
                .content(new Content().addMediaType("application/json", new MediaType()
                        .schema(new Schema<>().$ref(schemaRef))
                        .example(example)));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for AddTableEndpoint.
     */
    public static final class Builder {
        private InstanceProperties instanceProperties;
        private AddTable addTable;

        private Builder() {
        }

        /**
         * Assigns the instance properties.
         *
         * @param  instanceProperties the instance properties
         * @return                    the builder for further actions
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Assigns the add table operation.
         *
         * @param  addTable the add table operation
         * @return          the builder for further actions
         */
        public Builder addTable(AddTable addTable) {
            this.addTable = addTable;
            return this;
        }

        public AddTableRoute build() {
            return new AddTableRoute(this);
        }
    }

}
