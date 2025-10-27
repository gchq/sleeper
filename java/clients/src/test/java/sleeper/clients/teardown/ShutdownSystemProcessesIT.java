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
import sleeper.core.util.StaticRateLimit;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.anyRequestedForCloudWatchEvents;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequest;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequestedFor;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithNoApplications;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listActiveApplicationsRequested;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listActiveEmrApplicationsRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.aResponseWithNoClusters;
import static sleeper.clients.testutil.WiremockEmrTestHelper.aResponseWithNumRunningClusters;
import static sleeper.clients.testutil.WiremockEmrTestHelper.anyRequestedForEmr;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveClustersRequested;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveEmrClustersRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestWithJobIdCount;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestedFor;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestedWithJobIdsCount;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_METRICS_RULE;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.util.ThreadSleepTestHelper.noWaits;

@WireMockTest
class ShutdownSystemProcessesIT {

    private final InstanceProperties properties = createTestInstanceProperties();
    private ShutdownSystemProcesses shutdown;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        shutdown = new ShutdownSystemProcesses(wiremockCloudWatchClient(runtimeInfo),
                wiremockEmrClient(runtimeInfo), StaticRateLimit.none(), noWaits());
    }

    private void shutdown() throws Exception {
        shutdownWithExtraEcsClusters(List.of());
    }

    private void shutdownWithExtraEcsClusters(List<String> extraECSClusters) throws Exception {
        shutdown.shutdown(properties, extraECSClusters);
    }

    @Test
    void shouldPerformNoShutdownWhenNothingToDo() throws Exception {
        // Given
        stubFor(listActiveEmrClustersRequest()
                .willReturn(aResponseWithNoClusters()));
        stubFor(listActiveEmrApplicationsRequest()
                .willReturn(aResponseWithNoApplications()));

        // When
        shutdown();

        // Then
        verify(2, anyRequestedFor(anyUrl()));
        verify(1, listActiveClustersRequested());
        verify(1, listActiveApplicationsRequested());
    }

    @Nested
    @DisplayName("Shutdown Cloud Watch Rules")
    class ShutdownCloudWatchRules {

        @BeforeEach
        void setup() {
            stubFor(listActiveEmrClustersRequest()
                    .willReturn(aResponseWithNoClusters()));
            stubFor(listActiveEmrApplicationsRequest()
                    .willReturn(aResponseWithNoApplications()));
        }

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

    @Nested
    @DisplayName("Terminate running EMR clusters")
    class TerminateEMRClusters {

        @BeforeEach
        void setup() {
            properties.set(ID, "test-instance");
            stubFor(listActiveEmrApplicationsRequest()
                    .willReturn(aResponseWithNoApplications()));
        }

        @Test
        void shouldTerminateEMRClusterWhenOneClusterIsRunning() throws Exception {
            // Given
            stubFor(listActiveEmrClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponse().withStatus(200).withBody("" +
                            "{\"Clusters\": [{" +
                            "   \"Name\": \"sleeper-test-instance-test-cluster\"," +
                            "   \"Id\": \"test-cluster-id\"," +
                            "   \"Status\": {\"State\": \"RUNNING\"}" +
                            "}]}"))
                    .whenScenarioStateIs(STARTED));
            stubFor(terminateJobFlowsRequest().inScenario("TerminateEMRClusters")
                    .whenScenarioStateIs(STARTED)
                    .willSetStateTo("TERMINATED"));
            stubFor(listActiveEmrClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponse().withStatus(200).withBody(
                            "{\"Clusters\": []}"))
                    .whenScenarioStateIs("TERMINATED"));

            // When
            shutdown();

            // Then
            verify(3, anyRequestedForEmr());
            verify(2, listActiveClustersRequested());
            verify(1, terminateJobFlowsRequestedFor("test-cluster-id"));
        }

        @Test
        void shouldTerminateEMRClustersInBatchesOfTen() throws Exception {
            // Given
            stubFor(listActiveEmrClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponseWithNumRunningClusters(11))
                    .whenScenarioStateIs(STARTED));
            stubFor(terminateJobFlowsRequestWithJobIdCount(10).inScenario("TerminateEMRClusters")
                    .whenScenarioStateIs(STARTED).willSetStateTo("PART_TERMINATED"));
            stubFor(terminateJobFlowsRequestWithJobIdCount(1).inScenario("TerminateEMRClusters")
                    .whenScenarioStateIs("PART_TERMINATED").willSetStateTo("TERMINATED"));
            stubFor(listActiveEmrClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponseWithNoClusters())
                    .whenScenarioStateIs("TERMINATED"));

            // When
            shutdown();

            // Then
            verify(4, anyRequestedForEmr());
            verify(2, listActiveClustersRequested());
            verify(1, terminateJobFlowsRequestedWithJobIdsCount(10));
            verify(1, terminateJobFlowsRequestedWithJobIdsCount(1));
        }

        @Test
        void shouldNotTerminateEMRClusterWhenClusterBelongsToAnotherInstance() throws Exception {
            // Given
            stubFor(listActiveEmrClustersRequest()
                    .willReturn(aResponse().withStatus(200).withBody("" +
                            "{\"Clusters\": [{" +
                            "   \"Name\": \"sleeper-another-instance-test-cluster\"," +
                            "   \"Id\": \"test-cluster-id\"," +
                            "   \"Status\": {\"State\": \"RUNNING\"}" +
                            "}]}")));

            // When
            shutdown();

            // Then
            verify(1, anyRequestedForEmr());
            verify(1, listActiveClustersRequested());
        }
    }
}
