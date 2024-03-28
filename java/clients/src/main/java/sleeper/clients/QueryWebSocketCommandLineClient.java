/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.clients;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.instance.CdkDefinedInstanceProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.statestore.StateStoreException;
import sleeper.core.table.TableIndex;
import sleeper.query.model.Query;

import java.util.UUID;
import java.util.function.Supplier;

public class QueryWebSocketCommandLineClient extends QueryCommandLineClient {
    private final String apiUrl;
    private final QueryWebSocketClient queryWebSocketClient;

    private QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out) {
        this(instanceProperties, tableIndex, tablePropertiesProvider, in, out,
                new QueryWebSocketClient(instanceProperties, tablePropertiesProvider, out));
    }

    private QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, QueryWebSocketClient client) {
        this(instanceProperties, tableIndex, tablePropertiesProvider, in, out, client, () -> UUID.randomUUID().toString());
    }

    QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, QueryWebSocketClient client, Supplier<String> queryIdSupplier) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out, queryIdSupplier);

        this.apiUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL);
        if (this.apiUrl == null) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance!");
        }
        this.queryWebSocketClient = client;
    }

    @Override
    protected void init(TableProperties tableProperties) {
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) {
        try {
            queryWebSocketClient.submitQuery(query).join();
        } catch (InterruptedException e) {
            out.println("Failed to run query: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws StateStoreException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        QueryWebSocketCommandLineClient client = new QueryWebSocketCommandLineClient(instanceProperties,
                new DynamoDBTableIndex(instanceProperties, dynamoDBClient),
                new TablePropertiesProvider(instanceProperties, amazonS3, dynamoDBClient),
                new ConsoleInput(System.console()), new ConsoleOutput(System.out));
        client.run();
    }
}
