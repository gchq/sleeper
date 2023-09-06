/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.suite.dsl.query;

import sleeper.core.record.Record;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.query.SQSQueryDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.util.UUID;
import java.util.stream.Stream;

public class SystemTestSQSQuery {
    private final SystemTestQueryResults systemTestQueryResults;
    private final SQSQueryDriver sqsQueryDriver;
    private String queryId;

    public SystemTestSQSQuery(SleeperInstanceContext instance, SystemTestClients clients) {
        this.systemTestQueryResults = new SystemTestQueryResults(instance, clients);
        this.sqsQueryDriver = new SQSQueryDriver(instance, clients.getSqs());
    }

    public SystemTestSQSQuery allRecordsInTable() {
        String queryId = UUID.randomUUID().toString();
        sqsQueryDriver.allRecords(queryId);
        this.queryId = queryId;
        return this;
    }

    public SystemTestSQSQuery run(String key, Object min, Object max) {
        String queryId = UUID.randomUUID().toString();
        sqsQueryDriver.run(queryId, key, min, max);
        this.queryId = queryId;
        return this;
    }

    public SystemTestSQSQuery run(String key, Object min1, Object max1, Object min2, Object max2) {
        String queryId = UUID.randomUUID().toString();
        sqsQueryDriver.run(queryId, key, min1, max1, min2, max2);
        this.queryId = queryId;
        return this;
    }

    public SystemTestSQSQuery waitForQuery() throws InterruptedException {
        systemTestQueryResults.waitForQuery(queryId);
        return this;
    }

    public Stream<Record> results() {
        return systemTestQueryResults.results(queryId);
    }
}
