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

package sleeper.systemtest.dsl.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.record.Record;
import sleeper.query.core.model.Query;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public class QueryAllTablesSendAndWaitDriver implements QueryAllTablesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryAllTablesSendAndWaitDriver.class);

    private final SystemTestInstanceContext instance;
    private final QuerySendAndWaitDriver driver;

    public QueryAllTablesSendAndWaitDriver(SystemTestInstanceContext instance, QuerySendAndWaitDriver driver) {
        this.instance = instance;
        this.driver = driver;
    }

    @Override
    public Map<String, List<Record>> runForAllTables(Function<QueryCreator, Query> queryFactory) {
        List<Query> queries = QueryCreator.forAllTables(instance, queryFactory);
        LOGGER.info("Sending {} queries, one for each table", queries.size());
        queries.stream().parallel().forEach(driver::send);
        LOGGER.info("Waiting for {} queries", queries.size());
        for (Query query : queries) {
            driver.waitFor(query);
        }
        LOGGER.info("Retrieving results for {} queries", queries.size());
        return queries.stream().parallel()
                .map(query -> entry(instance.getTestTableName(query.getTableName()), driver.getResults(query)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public List<Record> run(Query query) {
        return driver.run(query);
    }
}
