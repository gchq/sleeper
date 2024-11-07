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

package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.record.Record;
import sleeper.query.core.model.Query;
import sleeper.query.runner.recordretrieval.InMemoryDataStore;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.query.QueryAllTablesDriver;
import sleeper.systemtest.dsl.query.QueryAllTablesSendAndWaitDriver;
import sleeper.systemtest.dsl.query.QueryDriver;
import sleeper.systemtest.dsl.query.QuerySendAndWaitDriver;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InMemoryQueryByQueueDriver implements QuerySendAndWaitDriver {

    private final QueryDriver driver;
    private final Set<String> inFlightQueryIds = Collections.synchronizedSet(new HashSet<>());
    private final Set<String> completedQueryIds = Collections.synchronizedSet(new HashSet<>());

    private InMemoryQueryByQueueDriver(SystemTestInstanceContext instance, InMemoryDataStore dataStore) {
        driver = new InMemoryDirectQueryDriver(instance, dataStore);
    }

    public static QueryAllTablesDriver allTablesDriver(SystemTestInstanceContext instance, InMemoryDataStore dataStore) {
        return new QueryAllTablesSendAndWaitDriver(instance, new InMemoryQueryByQueueDriver(instance, dataStore));
    }

    @Override
    public void send(Query query) {
        inFlightQueryIds.add(query.getQueryId());
    }

    @Override
    public void waitFor(Query query) {
        if (!inFlightQueryIds.remove(query.getQueryId())) {
            throw new IllegalStateException("Query not in-flight");
        }
        completedQueryIds.add(query.getQueryId());
    }

    @Override
    public List<Record> getResults(Query query) {
        if (!completedQueryIds.remove(query.getQueryId())) {
            throw new IllegalStateException("Query not completed");
        }
        return driver.run(query);
    }
}
