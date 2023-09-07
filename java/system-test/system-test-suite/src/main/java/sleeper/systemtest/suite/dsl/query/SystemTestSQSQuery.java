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
import sleeper.systemtest.drivers.query.QueryCreator;
import sleeper.systemtest.drivers.query.QueryRange;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.drivers.query.SQSQueryDriver;
import sleeper.systemtest.drivers.query.WaitForQueryDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.util.List;
import java.util.stream.Collectors;

public class SystemTestSQSQuery {
    private final QueryCreator queryCreator;
    private final SQSQueryDriver sqsQueryDriver;
    private final S3ResultsDriver s3ResultsDriver;
    private final WaitForQueryDriver waitForQueryDriver;

    public SystemTestSQSQuery(SleeperInstanceContext instance, SystemTestClients clients) {
        this.queryCreator = new QueryCreator(instance);
        this.sqsQueryDriver = new SQSQueryDriver(instance, clients.getSqs());
        this.s3ResultsDriver = new S3ResultsDriver(instance, clients.getS3());
        this.waitForQueryDriver = new WaitForQueryDriver(instance, clients.getDynamoDB());
    }

    public List<Record> allRecordsInTable() throws InterruptedException {
        return runAndReturn(queryCreator.allRecordsQuery());
    }

    public List<Record> byRowKey(String key, QueryRange... ranges) throws InterruptedException {
        return runAndReturn(queryCreator.byRowKey(key, List.of(ranges)));
    }

    private List<Record> runAndReturn(Query query) throws InterruptedException {
        sqsQueryDriver.run(query);
        waitForQueryDriver.waitForQuery(query.getQueryId());
        return s3ResultsDriver.results(query.getQueryId())
                .collect(Collectors.toUnmodifiableList());
    }
}
