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
package sleeper.clients.query;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.CdkDefinedInstanceProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.util.LoggedDuration;
import sleeper.query.core.model.Query;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

public class QueryWebSocketCommandLineClient extends QueryCommandLineClient {
    private final String apiUrl;
    private final QueryWebSocketClient queryWebSocketClient;
    private final Supplier<Instant> timeSupplier;

    QueryWebSocketCommandLineClient(
            InstanceProperties instanceProperties, TableIndex tableIndex, TablePropertiesProvider tablePropertiesProvider,
            ConsoleInput in, ConsoleOutput out, QueryWebSocketClient client, Supplier<String> queryIdSupplier,
            Supplier<Instant> timeSupplier) {
        super(instanceProperties, tableIndex, tablePropertiesProvider, in, out, queryIdSupplier);

        this.apiUrl = instanceProperties.get(CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL);
        if (this.apiUrl == null) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance!");
        }
        this.queryWebSocketClient = client;
        this.timeSupplier = timeSupplier;
    }

    @Override
    protected void init(TableProperties tableProperties) {
    }

    @Override
    protected void submitQuery(TableProperties tableProperties, Query query) throws InterruptedException {
        Instant startTime = timeSupplier.get();
        long recordsReturned = 0L;
        try {
            out.println("Submitting query with ID: " + query.getQueryId());
            List<String> results = queryWebSocketClient.submitQuery(query).join();
            out.println("Query results:");
            results.forEach(out::println);
            recordsReturned = results.size();
        } catch (CompletionException e) {
            out.println("Query failed: " + e.getCause().getMessage());
        } catch (RuntimeException | InterruptedException e) {
            out.println("Query failed: " + e.getMessage());
            throw e;
        } finally {
            out.println("Query took " + LoggedDuration.withFullOutput(startTime, timeSupplier.get()) + " to return " + recordsReturned + " records");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (1 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        String instanceId = args[0];

        try (S3Client s3Client = S3Client.create();
                DynamoDbClient dynamoClient = DynamoDbClient.create()) {
            AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            QueryWebSocketClient webSocketClient = new QueryWebSocketClient(instanceProperties, tablePropertiesProvider, credentialsProvider);
            QueryWebSocketCommandLineClient commandLineClient = new QueryWebSocketCommandLineClient(instanceProperties,
                    new DynamoDBTableIndex(instanceProperties, dynamoClient), tablePropertiesProvider,
                    new ConsoleInput(System.console()), new ConsoleOutput(System.out),
                    webSocketClient, () -> UUID.randomUUID().toString(), Instant::now);
            commandLineClient.run();
        }
    }
}
