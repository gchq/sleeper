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

package sleeper.systemtest.dsl.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.row.Row;
import sleeper.query.core.model.QueryProcessingConfig;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class QueryDsl {
    private final SystemTestContext context;
    private final SystemTestDrivers baseDrivers;
    private final SystemTestDrivers adminDrivers;
    private QueryAllTablesDriver driver = null;

    public QueryDsl(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.context = context;
        this.baseDrivers = baseDrivers;
        this.adminDrivers = context.instance().adminRoleDrivers();
    }

    public QueryDsl byQueue() {
        driver = adminDrivers.queryByQueue(context);
        return this;
    }

    public QueryDsl direct() {
        driver = adminDrivers.directQuery(context);
        return this;
    }

    public QueryDsl webSocket() {
        // Note that this relies on permissions of the base credentials,
        // as the instance admin does not currently have working permissions for the web socket.
        // TODO add the correct permissions to the instance admin role
        driver = baseDrivers.queryByWebSocket(context);
        return this;
    }

    public List<Row> allRowsInTable() {
        return driver.run(queryCreator().allRowsQuery());
    }

    public BackgroundQueries startContinuousQueries() {
        BackgroundQueries queries = new BackgroundQueries(this);
        queries.start();
        return queries;
    }

    public Map<String, List<Row>> allRowsByTable() {
        return driver.runForAllTables(QueryCreator::allRowsQuery);
    }

    public List<Row> byRowKey(String key, QueryRange... ranges) {
        return driver.run(queryCreator().byRowKey(key, List.of(ranges)));
    }

    public TimedQueryResultsDsl timedByRowKey(String key, QueryRange... ranges) {
        Instant startTime = Instant.now();
        List<Row> rows = byRowKey(key, ranges);
        Instant endTime = Instant.now();
        return new TimedQueryResultsDsl(startTime, endTime, rows);
    }

    public List<Row> allRowsWithProcessingConfig(
            Consumer<QueryProcessingConfig.Builder> config) {
        QueryProcessingConfig.Builder builder = QueryProcessingConfig.builder();
        config.accept(builder);
        return driver.run(queryCreator().allRowsQuery(builder.build()));
    }

    public void emptyResultsBucket() {
        adminDrivers.clearQueryResults(context).deleteAllQueryResults();
    }

    public static class BackgroundQueries {

        private static final Logger LOGGER = LoggerFactory.getLogger(BackgroundQueries.class);

        private final AtomicBoolean stopQueries = new AtomicBoolean(false);
        private final AtomicInteger queriesCompleted = new AtomicInteger();
        private final List<Throwable> queryFailures = new CopyOnWriteArrayList<>();

        private final Thread queryThread;

        private BackgroundQueries(QueryDsl queryDsl) {

            queryThread = new Thread(() -> {
                try {
                    while (!stopQueries.get()) {
                        queryDsl.allRowsInTable();

                        queriesCompleted.incrementAndGet();

                        Thread.sleep(100);
                    }
                } catch (Throwable t) {
                    LOGGER.error("Query thread failed", t);
                    queryFailures.add(t);
                }
            }, "background-query-thread");
        }

        private void start() {
            queryThread.start();
        }

        public void stop() {
            stopQueries.set(true);
        }

        public void stopAndWait() throws InterruptedException {
            stopQueries.set(true);
            queryThread.join();
        }

        public int getQueriesCompleted() {
            return queriesCompleted.get();
        }

        public List<Throwable> getFailures() {
            return queryFailures;
        }

        public void assertNoFailures() {
            if (!queryFailures.isEmpty()) {
                throw new RuntimeException(
                        "Background query thread failed",
                        queryFailures.get(0));
            }
        }
    }

    private QueryCreator queryCreator() {
        return new QueryCreator(context.instance());
    }
}
