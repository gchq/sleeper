/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.systemtest.drivers.testutil;

import software.amazon.awssdk.regions.Region;

import sleeper.localstack.test.SleeperLocalStackClients;
import sleeper.localstack.test.SleeperLocalStackContainer;
import sleeper.systemtest.drivers.compaction.AwsCompactionDriver;
import sleeper.systemtest.drivers.util.AwsSystemTestDrivers;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.drivers.util.sqs.AwsDrainSqsQueue;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;
import sleeper.systemtest.dsl.util.NoSnapshotsDriver;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import static sleeper.localstack.test.LocalStackHadoopConfigurationProvider.configureHadoop;

public class LocalStackSystemTestDrivers extends AwsSystemTestDrivers {
    private final SystemTestClients clients;

    private LocalStackSystemTestDrivers(SystemTestClients clients) {
        super(clients);
        this.clients = clients;
    }

    public static LocalStackSystemTestDrivers fromContainer() {
        return new LocalStackSystemTestDrivers(SystemTestClients.builder()
                .regionProvider(() -> Region.of(SleeperLocalStackContainer.INSTANCE.getRegion()))
                .s3(SleeperLocalStackClients.S3_CLIENT_V2)
                .s3Async(SleeperLocalStackClients.S3_ASYNC_CLIENT)
                .dynamo(SleeperLocalStackClients.DYNAMO_CLIENT_V2)
                .sqs(SleeperLocalStackClients.SQS_CLIENT_V2)
                .configureHadoopSetter(conf -> configureHadoop(conf, SleeperLocalStackContainer.INSTANCE))
                .skipAssumeRole(true)
                .build());
    }

    @Override
    public SystemTestDeploymentDriver systemTestDeployment(SystemTestParameters parameters) {
        return new LocalStackSystemTestDeploymentDriver(parameters, clients);
    }

    @Override
    public SleeperInstanceDriver instance(SystemTestParameters parameters) {
        return new LocalStackSleeperInstanceDriver(parameters, clients);
    }

    @Override
    public CompactionDriver compaction(SystemTestContext context) {
        return new AwsCompactionDriver(context.instance(), clients,
                AwsDrainSqsQueue.forLocalStackTests(clients.getSqs()));
    }

    @Override
    public SnapshotsDriver snapshots() {
        return new NoSnapshotsDriver();
    }

    @Override
    public PollWithRetriesDriver pollWithRetries() {
        return PollWithRetriesDriver.noWaits();
    }

    public SystemTestClients clients() {
        return clients;
    }
}
