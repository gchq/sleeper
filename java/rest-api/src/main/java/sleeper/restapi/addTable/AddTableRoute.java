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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.AddTable;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.restapi.Route;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
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
        instanceProperties = requireNonNull(builder.instanceProperties);
        addTable = requireNonNull(builder.addTable);
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
