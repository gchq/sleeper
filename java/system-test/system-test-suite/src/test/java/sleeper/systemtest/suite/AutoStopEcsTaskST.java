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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.model.OptionalStack;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.systemtest.configuration.SystemTestIngestMode.GENERATE_ONLY;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.REENABLE_OPTIONAL_STACKS;

@SystemTest
// Slow because it needs to do multiple CDK deployments
@Slow
public class AutoStopEcsTaskST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstanceAddOnlineTable(REENABLE_OPTIONAL_STACKS);
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.disableOptionalStacks(OptionalStack.all());
    }

    @Test
    void shouldRemoveIngestStackWhileTaskIsRunning(SleeperSystemTest sleeper) {
        // Given an ingest task is running
        sleeper.enableOptionalStacks(List.of(OptionalStack.IngestStack));
        sleeper.systemTestCluster()
                .runDataGenerationJobs(10,
                        builder -> builder.ingestMode(GENERATE_ONLY).rowsPerIngest(40_000_000),
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(20)))
                .sendAllGeneratedFilesAsOneJob(INGEST_JOB_QUEUE_URL)
                .waitForStandardIngestTasks(1,
                        PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(10)));

        // When I remove the ingest stack
        sleeper.disableOptionalStacks(List.of(OptionalStack.IngestStack));

        // Then the ingest does not complete
        assertThat(sleeper.tableFiles().references()).isEmpty();
    }
}
