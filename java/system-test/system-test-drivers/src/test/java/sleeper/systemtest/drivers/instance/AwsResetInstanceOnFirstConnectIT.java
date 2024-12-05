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
package sleeper.systemtest.drivers.instance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.testutil.AwsSendCompactionJobsTestHelper;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.drivers.util.AwsDrainSqsQueue;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SleeperTablesDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.LOCALSTACK_MAIN;
import static sleeper.systemtest.dsl.util.SystemTestSchema.DEFAULT_SCHEMA;

@LocalStackDslTest
public class AwsResetInstanceOnFirstConnectIT {

    SystemTestInstanceContext instance;
    InstanceProperties instanceProperties;
    SleeperInstanceDriver instanceDriver;
    SleeperTablesDriver tablesDriver;
    AwsResetInstanceOnFirstConnect onFirstConnect;
    SqsClient sqs;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, LocalStackSystemTestDrivers drivers, SystemTestContext context, SystemTestParameters parameters) {
        sleeper.connectToInstanceNoTables(LOCALSTACK_MAIN);
        instance = context.instance();
        instanceProperties = sleeper.instanceProperties();
        instanceDriver = drivers.instance(parameters);
        tablesDriver = drivers.tables(parameters);
        onFirstConnect = new AwsResetInstanceOnFirstConnect(drivers.clients());
        sqs = drivers.clients().getSqsV2();
    }

    @Test
    void shouldDeleteOneTable(SleeperSystemTest sleeper) {
        sleeper.tables().create("A", DEFAULT_SCHEMA);
        onFirstConnect.reset(instanceProperties);
        assertThat(tablesDriver.tableIndex(instanceProperties).streamAllTables())
                .isEmpty();
    }

    @Test
    void shouldDrainQueues(SleeperSystemTest sleeper) {
        // Given
        sleeper.tables().create("A", DEFAULT_SCHEMA);
        AwsSendCompactionJobsTestHelper.sendNCompactionJobs(20,
                sleeper.instanceProperties(), sleeper.tableProperties(), instance.getStateStore(), sqs);

        // When
        onFirstConnect.reset(instanceProperties);

        // Then
        assertThat(AwsDrainSqsQueue.drainQueueForWholeInstance(sqs, instanceProperties.get(COMPACTION_JOB_QUEUE_URL)))
                .isEmpty();
    }

    @Test
    void shouldDeleteNothingWhenNoTablesArePresent() {
        assertThatCode(() -> onFirstConnect.reset(instanceProperties))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldDeleteNothingWhenNoTablesArePresentAndInstancePropertiesAreSavedInConfigBucket(SystemTestDrivers drivers) {
        instanceDriver.saveInstanceProperties(instanceProperties);
        onFirstConnect.reset(instanceProperties);

        InstanceProperties loaded = new InstanceProperties();
        instanceDriver.loadInstanceProperties(loaded, instanceProperties.get(ID));
        assertThat(loaded).isEqualTo(instanceProperties);
    }

}
