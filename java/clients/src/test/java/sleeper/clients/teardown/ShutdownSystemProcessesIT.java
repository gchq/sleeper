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
package sleeper.clients.teardown;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.anyRequestedForCloudWatchEvents;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequest;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequestedFor;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@WireMockTest
class ShutdownSystemProcessesIT {

    private final InstanceProperties properties = createTestInstanceProperties();
    private ShutdownSystemProcesses shutdown;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        shutdown = new ShutdownSystemProcesses(wiremockCloudWatchClient(runtimeInfo));
    }

    private void shutdown() throws Exception {
        shutdownWithExtraEcsClusters(List.of());
    }

    private void shutdownWithExtraEcsClusters(List<String> extraECSClusters) throws Exception {
        shutdown.shutdown(properties, extraECSClusters);
    }

    @Nested
    @DisplayName("Shutdown Cloud Watch Rules")
    class ShutdownCloudWatchRules {

        @Test
        void shouldShutdownCloudWatchRulesWhenSet() throws Exception {
            // Given
            properties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-compaction-job-creation-rule");
            properties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-compaction-task-creation-rule");
            properties.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, "test-partition-splitting-rule");
            properties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-garbage-collector-rule");
            properties.set(INGEST_CLOUDWATCH_RULE, "test-ingest-task-creation-rule");
            properties.set(TABLE_METRICS_RULE, "test-table-metrics-rule");

            stubFor(disableRuleRequest()
                    .willReturn(aResponse().withStatus(200)));

            // When
            shutdown();

            // Then
            verify(6, anyRequestedForCloudWatchEvents());
            verify(1, disableRuleRequestedFor("test-compaction-job-creation-rule"));
            verify(1, disableRuleRequestedFor("test-compaction-task-creation-rule"));
            verify(1, disableRuleRequestedFor("test-partition-splitting-rule"));
            verify(1, disableRuleRequestedFor("test-garbage-collector-rule"));
            verify(1, disableRuleRequestedFor("test-ingest-task-creation-rule"));
            verify(1, disableRuleRequestedFor("test-table-metrics-rule"));
        }
    }

}
