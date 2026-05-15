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

import sleeper.clients.api.SleeperClient;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableAlreadyExistsException;
import sleeper.restapi.RestEndpoint;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Rest method for adding table to sleeper instance, utilising same process as within the Sleeper Client.
 */
public class AddTableEndpoint extends RestEndpoint {

    public static final Logger LOGGER = LoggerFactory.getLogger(AddTableEndpoint.class);
    private InstanceProperties instanceProperties;
    private SleeperClient sleeperClient;
    private AddTableSerDe addTableSerDe;

    private AddTableEndpoint(Builder builder) {
        instanceProperties = requireNonNull(builder.instanceProperties);
        sleeperClient = requireNonNull(builder.sleeperClient);
        addTableSerDe = new AddTableSerDe();
    }

    /**
     * Decodes details for the AddTableRequest and then actions them within the application
     *
     * @param  event APIGateway passing into this method from the determined rest api route
     * @return       An APIGateway response detailing whether the addTable action has suceeded or not
     */
    @Override
    public APIGatewayV2HTTPResponse processRequest(APIGatewayV2HTTPEvent event) {
        AddTableRequest request;
        try {
            request = addTableSerDe.fromJson(decodeBody(event));
        } catch (JsonSyntaxException e) {
            LOGGER.warn("Add table request body was not valid JSON", e);
            return errorResponse(400, "invalid_request", "Request body is not valid JSON");
        }
        if (request == null) {
            return errorResponse(400, "invalid_request", "Request body is empty");
        }
        TableProperties tableProperties;
        List<Object> splitPoints;
        try {
            tableProperties = request.toTableProperties(instanceProperties);
            splitPoints = request.toSplitPoints(tableProperties);
        } catch (RuntimeException e) {
            // SchemaSerDe / split-point parsing surface invalid input as runtime exceptions.
            LOGGER.warn("Add table request was invalid", e);
            return errorResponse(400, "invalid_request", e.getMessage());
        }
        try {
            sleeperClient.addTable(tableProperties, splitPoints);
        } catch (TableAlreadyExistsException e) {
            LOGGER.info("Add table request rejected: table already exists", e);
            return errorResponse(409, "table_already_exists", e.getMessage());
        } catch (IllegalArgumentException e) {
            // SleeperPropertiesInvalidException extends IllegalArgumentException.
            LOGGER.warn("Add table request rejected: properties failed validation", e);
            return errorResponse(400, "invalid_request", e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Add table request failed unexpectedly", e);
            return errorResponse(500, "internal_error", "Failed to add table");
        }
        AddTableResponse response = AddTableResponse.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .tableName(tableProperties.get(TABLE_NAME))
                .build();
        return APIGatewayV2HTTPResponse.builder()
                .withStatusCode(201)
                .withHeaders(Map.of("Content-Type", CONTENT_TYPE_JSON))
                .withBody(addTableSerDe.toJson(response))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for AddTableRest.
     */
    public static final class Builder {
        private InstanceProperties instanceProperties;
        private SleeperClient sleeperClient;

        private Builder() {
        }

        /**
         * Assigns instance properties for rest method.
         *
         * @param  instanceProperties instanceProperties
         * @return                    the builder for further actions
         */
        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        /**
         * Assigns sleeper client for rest method.
         *
         * @param  sleeperClient sleeper client
         * @return               the builder for further actions
         */
        public Builder sleeperClient(SleeperClient sleeperClient) {
            this.sleeperClient = sleeperClient;
            return this;
        }

        public AddTableEndpoint build() {
            return new AddTableEndpoint(this);
        }
    }

}
