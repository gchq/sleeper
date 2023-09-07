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
import sleeper.systemtest.drivers.query.QueryRange;
import sleeper.systemtest.drivers.query.S3ResultsDriver;
import sleeper.systemtest.drivers.query.SQSQueryDriver;
import sleeper.systemtest.drivers.query.WaitForQueryDriver;
import sleeper.systemtest.suite.fixtures.SystemTestClients;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class SystemTestSQSQuery {
    private final SQSQueryDriver sqsQueryDriver;
    private final S3ResultsDriver s3ResultsDriver;
    private final WaitForQueryDriver waitForQueryDriver;

    public SystemTestSQSQuery(SleeperInstanceContext instance, SystemTestClients clients) {
        this.sqsQueryDriver = new SQSQueryDriver(instance, clients.getSqs());
        this.s3ResultsDriver = new S3ResultsDriver(instance, clients.getS3());
        this.waitForQueryDriver = new WaitForQueryDriver(instance, clients.getDynamoDB());
    }

    public List<Record> allRecordsInTable() throws InterruptedException {
        String queryId = UUID.randomUUID().toString();
        sqsQueryDriver.allRecords(queryId);
        return waitAndReturn(queryId);
    }

    public List<Record> byRowKey(String key, QueryRange... ranges) throws InterruptedException {
        String queryId = UUID.randomUUID().toString();
        sqsQueryDriver.run(queryId, key, List.of(ranges));
        return waitAndReturn(queryId);
    }

    private List<Record> waitAndReturn(String queryId) throws InterruptedException {
        waitForQueryDriver.waitForQuery(queryId);
        return s3ResultsDriver.results(queryId)
                .collect(Collectors.toUnmodifiableList());
    }
}
