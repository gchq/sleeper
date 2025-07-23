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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.util.LoggedDuration;
import sleeper.query.core.model.Query;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.QUERY_WEBSOCKET_API_URL;

public class QueryWebSocketClient {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryWebSocketClient.class);
    public static final long DEFAULT_TIMEOUT_MS = 120000L;

    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final AdapterProvider clientProvider;
    private final long timeoutMs;

    public QueryWebSocketClient(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider, AwsCredentialsProvider credentialsProvider) {
        this(instanceProperties, tablePropertiesProvider, QueryWebSocketClientAdapter.provider(credentialsProvider), DEFAULT_TIMEOUT_MS);
    }

    QueryWebSocketClient(InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            AdapterProvider clientProvider, long timeoutMs) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.clientProvider = clientProvider;
        this.timeoutMs = timeoutMs;
        if (!instanceProperties.isSet(QUERY_WEBSOCKET_API_URL)) {
            throw new IllegalArgumentException("Use of this query client requires the WebSocket API to have been deployed as part of your Sleeper instance.");
        }
    }

    public CompletableFuture<List<Row>> submitQuery(Query query) throws InterruptedException {
        TableProperties tableProperties = tablePropertiesProvider.getByName(query.getTableName());
        Adapter adapter = clientProvider.createClient(instanceProperties, tableProperties);
        try {
            Instant startTime = Instant.now();
            return adapter.startQueryFuture(query)
                    .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                    .whenComplete((rows, exception) -> {
                        LoggedDuration duration = LoggedDuration.withFullOutput(startTime, Instant.now());
                        LOGGER.info("Query took {} to return {} rows", duration, rows.size());
                        adapter.close();
                    });
        } catch (RuntimeException | InterruptedException e) {
            adapter.close();
            throw e;
        }
    }

    public interface Adapter {
        void close();

        CompletableFuture<List<Row>> startQueryFuture(Query query) throws InterruptedException;
    }

    public interface AdapterProvider {
        Adapter createClient(InstanceProperties instanceProperties, TableProperties tableProperties);
    }
}
