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
package sleeper.api.resources;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.api.PropertyUpdates;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.table.TableNotFoundException;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Path("/api/tables")
public class TablesResource {

    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    private final InstanceProperties instanceProperties;
    private final DynamoDBTableIndex tableIndex;

    @Inject
    public TablesResource(
        S3Client s3Client,
        DynamoDbClient dynamoDbClient,
        @ConfigProperty(name = "sleeper.instance.id") String instanceId,
        @ConfigProperty(name = "sleeper.account.name") String accountName
    ) {
        this.s3Client = s3Client;
        this.dynamoDbClient = dynamoDbClient;

        this.instanceProperties = S3InstanceProperties.loadGivenAccountAndInstanceId(s3Client, accountName, instanceId);
        this.tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoDbClient);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<TableStatus> getTables() {
        return this.tableIndex.streamAllTables().collect(Collectors.toList());
    }

    @GET
    @Path("/{tableId}/properties")
    @Produces(MediaType.APPLICATION_JSON)
    public Properties getTableProperties(@PathParam("tableId") String tableId) {
        TableProperties tableProperties = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDbClient).getById(tableId);
        return tableProperties.getProperties();
    }

    @POST
    @Path("/{tableId}/properties")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTableProperties(@PathParam("tableId") String tableId, Map<String, String> changes) {
        TablePropertiesStore store = S3TableProperties.createStore(instanceProperties, s3Client, dynamoDbClient);
        TableProperties tableProperties;
        try {
            tableProperties = store.loadById(tableId);
        } catch (TableNotFoundException e) {
            throw new WebApplicationException(e.getMessage(), Response.Status.NOT_FOUND);
        }
        PropertyUpdates.applyOrThrow(tableProperties, changes);
        store.save(tableProperties);
        return Response.noContent().build();
    }

}
