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
import sleeper.query.model.Query;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.query.DirectQueryDriver;
import sleeper.systemtest.drivers.query.QueryCreator;
import sleeper.systemtest.drivers.query.QueryDriver;
import sleeper.systemtest.drivers.query.QueryRange;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.drivers.query.SQSQueryDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

public class SystemTestQuery {
    private final SleeperInstanceContext instance;
    private final SystemTestClients clients;
    private QueryDriver driver = null;

    public SystemTestQuery(SleeperInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.clients = clients;
    }

    public SystemTestQuery byQueue() {
        driver = new SQSQueryDriver(instance, clients.getSqs(), clients.getDynamoDB(), clients.getS3());
        return this;
    }

    public SystemTestQuery direct() {
        driver = new DirectQueryDriver(instance);
        return this;
    }

    public List<Record> allRecordsInTable() throws InterruptedException {
        return driver.run(queryCreator().allRecordsQuery());
    }

    public Map<String, List<Record>> allRecordsByTable() throws InterruptedException {
        SQSQueryDriver driver = (SQSQueryDriver) this.driver;
        List<Query> queries = instance.streamTableNames()
                .map(tableName -> queryCreator(tableName).allRecordsQuery())
                .collect(Collectors.toUnmodifiableList());
        queries.stream().parallel().forEach(driver::send);
        for (Query query : queries) {
            driver.waitForQuery(query);
        }
        return queries.stream().parallel()
                .map(query -> entry(query.getTableName(), driver.getResults(query)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public List<Record> byRowKey(String key, QueryRange... ranges) throws InterruptedException {
        return driver.run(queryCreator().byRowKey(key, List.of(ranges)));
    }

    public void emptyResultsBucket() {
        new S3ResultsDriver(instance, clients.getS3()).emptyBucket();
    }

    private QueryCreator queryCreator() {
        return new QueryCreator(instance);
    }

    private QueryCreator queryCreator(String tableName) {
        return new QueryCreator(instance, tableName);
    }
}
