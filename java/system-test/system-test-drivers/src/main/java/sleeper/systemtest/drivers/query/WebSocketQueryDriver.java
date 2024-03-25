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
import sleeper.core.util.PollWithRetries;
import sleeper.query.model.Query;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QuerySendAndWaitDriver;

import java.time.Duration;
import java.util.List;

public class WebSocketQueryDriver implements QuerySendAndWaitDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketQueryDriver.class);

    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(1), Duration.ofMinutes(1));
    private final QueryWebSocketClient queryWebSocketClient;

    public WebSocketQueryDriver(SystemTestInstanceContext instance) {
        this.queryWebSocketClient = new QueryWebSocketClient(instance.getInstanceProperties(), instance.getTablePropertiesProvider());
    }

    @Override
    public void send(Query query) {
        LOGGER.info("Submitting query: {}", query.getQueryId());
        queryWebSocketClient.submitQuery(query);
    }

    @Override
    public void waitFor(Query query) {
        LOGGER.info("Waiting for query: {}", query.getQueryId());
        queryWebSocketClient.waitForQuery(poll);
    }

    @Override
    public List<Record> getResults(Query query) {
        LOGGER.info("Loading results for query: {}", query.getQueryId());
        return queryWebSocketClient.getResults(query);
    }

}
