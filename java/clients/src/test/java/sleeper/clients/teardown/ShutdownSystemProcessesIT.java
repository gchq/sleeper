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
package sleeper.clients.teardown;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;
import software.amazon.awssdk.services.emrserverless.model.JobRunState;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.StaticRateLimit;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static sleeper.clients.testutil.ClientWiremockTestHelper.OPERATION_HEADER;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEcsClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrServerlessClient;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.anyRequestedForCloudWatchEvents;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequest;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequestedFor;
import static sleeper.clients.testutil.WiremockEcsTestHelper.MATCHING_LIST_TASKS_OPERATION;
import static sleeper.clients.testutil.WiremockEcsTestHelper.MATCHING_STOP_TASK_OPERATION;
import static sleeper.clients.testutil.WiremockEcsTestHelper.anyRequestedForEcs;
import static sleeper.clients.testutil.WiremockEcsTestHelper.listTasksRequestedFor;
import static sleeper.clients.testutil.WiremockEcsTestHelper.stopTaskRequestedFor;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithApplicationWithNameAndState;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithApplicationWithState;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithJobRunWithState;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithNoApplications;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithNoJobRuns;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.anyRequestedForEmrServerless;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.cancelJobRunRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.cancelJobRunRequested;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listActiveApplicationsRequested;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listActiveEmrApplicationsRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listRunningJobsForApplicationRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listRunningJobsForApplicationRequested;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listRunningOrCancellingJobsForApplicationRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listRunningOrCancellingJobsForApplicationRequested;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.stopApplicationRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.stopApplicationRequested;
import static sleeper.clients.testutil.WiremockEmrTestHelper.aResponseWithNoClusters;
import static sleeper.clients.testutil.WiremockEmrTestHelper.aResponseWithNumRunningClusters;
import static sleeper.clients.testutil.WiremockEmrTestHelper.anyRequestedForEmr;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveClustersRequested;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveEmrClustersRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestWithJobIdCount;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestedFor;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestedWithJobIdsCount;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
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
        shutdown = new ShutdownSystemProcesses(wiremockCloudWatchClient(runtimeInfo), wiremockEcsClient(runtimeInfo),
                wiremockEmrClient(runtimeInfo), wiremockEmrServerlessClient(runtimeInfo), StaticRateLimit.none(), noWaits());
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
            properties.set(TABLE_METRICS_RULE, "test-table-metrics-rule-1,test-table-metrics-rule-2");

            stubFor(disableRuleRequest()
                    .willReturn(aResponse().withStatus(200)));

            // When
            shutdown();

            // Then
            verify(7, anyRequestedForCloudWatchEvents());
            verify(1, disableRuleRequestedFor("test-compaction-job-creation-rule"));
            verify(1, disableRuleRequestedFor("test-compaction-task-creation-rule"));
            verify(1, disableRuleRequestedFor("test-partition-splitting-rule"));
            verify(1, disableRuleRequestedFor("test-garbage-collector-rule"));
            verify(1, disableRuleRequestedFor("test-ingest-task-creation-rule"));
            verify(1, disableRuleRequestedFor("test-table-metrics-rule-1"));
            verify(1, disableRuleRequestedFor("test-table-metrics-rule-2"));
        }
    }

    @Nested
    @DisplayName("Terminate running ECS tasks")
    class TerminateECSTasks {

        @BeforeEach
        void setup() {
            properties.set(INGEST_CLUSTER, "test-ingest-cluster");
            stubFor(listActiveEmrClustersRequest()
                    .willReturn(aResponseWithNoClusters()));
            stubFor(listActiveEmrApplicationsRequest()
                    .willReturn(aResponseWithNoApplications()));
        }

        @Test
        void shouldLookForECSTasksWhenClustersSet() throws Exception {
            // Given
            properties.set(COMPACTION_CLUSTER, "test-compaction-cluster");
            List<String> extraECSClusters = List.of("test-system-test-cluster");

            stubFor(post("/")
                    .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                    .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[]}")));

            // When
            shutdownWithExtraEcsClusters(extraECSClusters);

            // Then
            verify(3, anyRequestedForEcs());
            verify(1, listTasksRequestedFor("test-ingest-cluster"));
            verify(1, listTasksRequestedFor("test-compaction-cluster"));
            verify(1, listTasksRequestedFor("test-system-test-cluster"));
        }

        @Test
        void shouldStopECSTaskWhenOneIsFound() throws Exception {
            // Given
            stubFor(post("/")
                    .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                    .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[\"test-task\"]}")));
            stubFor(post("/")
                    .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                    .willReturn(aResponse().withStatus(200)));

            // When
            shutdown();

            // Then
            verify(2, anyRequestedForEcs());
            verify(1, listTasksRequestedFor("test-ingest-cluster"));
            verify(1, stopTaskRequestedFor("test-ingest-cluster", "test-task"));
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

    @Nested
    @DisplayName("Terminate running EMR Serverless Applications")
    class TerminateEMRServerlessApplications {

        @BeforeEach
        void setUp() {
            properties.set(ID, "test");
            stubFor(listActiveEmrClustersRequest()
                    .willReturn(aResponseWithNoClusters()));
        }

        @Test
        void shouldIgnoreARunningApplicationNotMatchingPrefix() throws Exception {
            // Given
            stubFor(listActiveEmrApplicationsRequest()
                    .willReturn(aResponseWithApplicationWithNameAndState("unmanaged-app", ApplicationState.STARTED)));

            // When
            shutdown();

            // Then
            verify(1, anyRequestedForEmrServerless());
            verify(1, listActiveApplicationsRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithRunningJob() throws Exception {
            // Given
            stubFor(listActiveEmrApplicationsRequest().inScenario("StopJob")
                    .willReturn(aResponseWithApplicationWithState(ApplicationState.STARTED))
                    .whenScenarioStateIs(STARTED));
            stubFor(listRunningJobsForApplicationRequest().inScenario("StopJob")
                    .willReturn(aResponseWithJobRunWithState("test-job-run", JobRunState.RUNNING))
                    .whenScenarioStateIs(STARTED));
            stubFor(cancelJobRunRequest("test-job-run").inScenario("StopJob")
                    .willReturn(ResponseDefinitionBuilder.okForEmptyJson())
                    .whenScenarioStateIs(STARTED).willSetStateTo("JobStopped"));
            stubFor(listRunningOrCancellingJobsForApplicationRequest().inScenario("StopJob")
                    .willReturn(aResponseWithNoJobRuns())
                    .whenScenarioStateIs("JobStopped"));
            stubFor(stopApplicationRequest().inScenario("StopJob")
                    .willReturn(aResponse().withStatus(200))
                    .whenScenarioStateIs("JobStopped").willSetStateTo("AppStopped"));
            stubFor(listActiveEmrApplicationsRequest().inScenario("StopJob")
                    .willReturn(aResponseWithNoApplications())
                    .whenScenarioStateIs("AppStopped"));

            // When
            shutdown();

            // Then
            verify(6, anyRequestedForEmrServerless());
            verify(2, listActiveApplicationsRequested());
            verify(1, listRunningJobsForApplicationRequested());
            verify(1, cancelJobRunRequested("test-job-run"));
            verify(1, listRunningOrCancellingJobsForApplicationRequested());
            verify(1, stopApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithNoRunningJobs() throws Exception {
            // Given
            stubFor(listActiveEmrApplicationsRequest().inScenario("StopApplication")
                    .willReturn(aResponseWithApplicationWithState(ApplicationState.STARTED))
                    .whenScenarioStateIs(STARTED));
            stubFor(listRunningJobsForApplicationRequest()
                    .willReturn(aResponseWithNoJobRuns()));
            stubFor(stopApplicationRequest().inScenario("StopApplication")
                    .willReturn(aResponse().withStatus(200))
                    .whenScenarioStateIs(STARTED).willSetStateTo("ApplicationStopped"));
            stubFor(listActiveEmrApplicationsRequest().inScenario("StopApplication")
                    .willReturn(aResponseWithNoApplications())
                    .whenScenarioStateIs("ApplicationStopped"));

            // When
            shutdown();

            // Then
            verify(4, anyRequestedForEmrServerless());
            verify(2, listActiveApplicationsRequested());
            verify(1, listRunningJobsForApplicationRequested());
            verify(1, stopApplicationRequested());
        }
    }
}
