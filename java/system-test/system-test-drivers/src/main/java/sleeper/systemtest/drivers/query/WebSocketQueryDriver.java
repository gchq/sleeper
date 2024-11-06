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
import sleeper.core.record.Record;
import sleeper.core.record.serialiser.RecordJSONSerDe;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.Query;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesInParallelDriver;
import sleeper.systemtest.dsl.query.QueryDriver;

import java.util.List;
import java.util.stream.Collectors;

public class WebSocketQueryDriver implements QueryDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryDriver.class);

    private final SystemTestInstanceContext instance;
    private final QueryWebSocketClient queryWebSocketClient;

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance) {
        return new QueryAllTablesInParallelDriver(instance, new WebSocketQueryDriver(instance));
    }

    public WebSocketQueryDriver(SystemTestInstanceContext instance) {
        this.instance = instance;
        this.queryWebSocketClient = new QueryWebSocketClient(instance.getInstanceProperties(), instance.getTablePropertiesProvider());
    }

    @Override
    public List<Record> run(Query query) {
        LOGGER.info("Submitting query: {}", query.getQueryId());
        Schema schema = instance.getTablePropertiesByDeployedName(query.getTableName()).orElseThrow().getSchema();
        RecordJSONSerDe recordSerDe = new RecordJSONSerDe(schema);
        try {
            return queryWebSocketClient.submitQuery(query).join().stream()
                    .map(recordSerDe::fromJson)
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
