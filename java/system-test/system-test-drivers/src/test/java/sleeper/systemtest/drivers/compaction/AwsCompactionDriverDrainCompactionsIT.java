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
package sleeper.systemtest.drivers.compaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.systemtest.drivers.testutil.AwsSendCompactionJobsTestHelper;
import sleeper.systemtest.drivers.testutil.LocalStackDslTest;
import sleeper.systemtest.drivers.testutil.LocalStackSystemTestDrivers;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.compaction.CompactionDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.drivers.testutil.LocalStackTestInstance.DRAIN_COMPACTIONS;

@LocalStackDslTest
public class AwsCompactionDriverDrainCompactionsIT {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsCompactionDriverDrainCompactionsIT.class);

    SqsClient sqs;
    CompactionDriver driver;
    SystemTestInstanceContext instance;

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, SystemTestContext context, LocalStackSystemTestDrivers drivers) {
        sleeper.connectToInstance(DRAIN_COMPACTIONS);
        sqs = drivers.clients().getSqsV2();
        driver = drivers.compaction(context);
        instance = context.instance();
    }

    @Test
    void shouldDrainCompactionJobsFromQueue() {
        // Given
        List<CompactionJob> jobs = AwsSendCompactionJobsTestHelper.sendNCompactionJobs(20,
                instance.getInstanceProperties(), instance.getTableProperties(), instance.getStateStore(), sqs);

        // When / Then
        assertThat(driver.drainJobsQueueForWholeInstance())
                .containsExactlyElementsOf(jobs);
        assertThat(driver.drainJobsQueueForWholeInstance()).isEmpty();
    }

}
