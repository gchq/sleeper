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
package sleeper.systemtest.drivers.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.QueryWebSocketClient;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.record.serialiser.RecordJSONSerDe;
import sleeper.core.schema.Schema;
import sleeper.query.model.Query;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesSendAndWaitDriver;
import sleeper.systemtest.dsl.query.QuerySendAndWaitDriver;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class WebSocketQueryDriver implements QuerySendAndWaitDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryDriver.class);

    private final QueryWebSocketClient queryWebSocketClient;
    private CompletableFuture<List<String>> future;
    private TablePropertiesProvider tablePropertiesProvider;

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance) {
        return new QueryAllTablesSendAndWaitDriver(instance, new WebSocketQueryDriver(instance));
    }

    public WebSocketQueryDriver(SystemTestInstanceContext instance) {
        this.queryWebSocketClient = new QueryWebSocketClient(instance.getInstanceProperties(), instance.getTablePropertiesProvider());
        this.tablePropertiesProvider = instance.getTablePropertiesProvider();
    }

    @Override
    public void send(Query query) {
        LOGGER.info("Submitting query: {}", query.getQueryId());
        try {
            future = queryWebSocketClient.submitQuery(query);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void waitFor(Query query) {
        LOGGER.info("Waiting for query: {}", query.getQueryId());
        future.join();
    }

    @Override
    public List<Record> getResults(Query query) {
        LOGGER.info("Loading results for query: {}", query.getQueryId());
        Schema schema = tablePropertiesProvider.getByName(query.getTableName()).getSchema();
        RecordJSONSerDe recordSerDe = new RecordJSONSerDe(schema);
        return queryWebSocketClient.getResults(query).stream()
                .map(recordSerDe::fromJson)
                .collect(Collectors.toList());
    }

}
