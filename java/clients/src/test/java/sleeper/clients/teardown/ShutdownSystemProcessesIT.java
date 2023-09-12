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
package sleeper.clients.teardown;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emrserverless.model.ApplicationState;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static sleeper.clients.testutil.ClientWiremockTestHelper.OPERATION_HEADER;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrServerlessClient;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequest;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequestedFor;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithNumRunningApplications;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.aResponseWithNumRunningJobsOnApplication;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.deleteJobsForApplicationsRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listActiveApplicationRequested;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listActiveApplicationsRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.listJobsForApplicationsRequest;
import static sleeper.clients.testutil.WiremockEmrServerlessTestHelper.stopJobForApplicationsRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.aResponseWithNumRunningClusters;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveClustersRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.listActiveClustersRequested;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequest;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestWithJobIdCount;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestedFor;
import static sleeper.clients.testutil.WiremockEmrTestHelper.terminateJobFlowsRequestedWithJobIdsCount;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_METRICS_RULES;
import static sleeper.job.common.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
class ShutdownSystemProcessesIT {

    private static final StringValuePattern MATCHING_LIST_TASKS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListTasks");
    private static final StringValuePattern MATCHING_STOP_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.StopTask");

    private final InstanceProperties properties = createTestInstanceProperties();
    private ShutdownSystemProcesses shutdown;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        shutdown = new ShutdownSystemProcesses(wiremockCloudWatchClient(runtimeInfo), wiremockEcsClient(runtimeInfo),
                wiremockEmrClient(runtimeInfo), wiremockEmrServerlessClient(runtimeInfo));
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

        @BeforeEach
        void setup() {
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));
        }

        @Test
        void shouldShutdownCloudWatchRulesWhenSet() throws Exception {
            // Given
            properties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-compaction-job-creation-rule");
            properties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-compaction-task-creation-rule");
            properties.set(SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-splitting-compaction-task-creation-rule");
            properties.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, "test-partition-splitting-rule");
            properties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-garbage-collector-rule");
            properties.set(INGEST_CLOUDWATCH_RULE, "test-ingest-task-creation-rule");
            properties.set(TABLE_METRICS_RULES, "test-table-metrics-rule-1,test-table-metrics-rule-2");

            stubFor(disableRuleRequest()
                    .willReturn(aResponse().withStatus(200)));

            // When
            shutdown();

            // Then
            verify(9, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
            verify(1, disableRuleRequestedFor("test-compaction-job-creation-rule"));
            verify(1, disableRuleRequestedFor("test-compaction-task-creation-rule"));
            verify(1, disableRuleRequestedFor("test-splitting-compaction-task-creation-rule"));
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
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));
        }

        @Test
        void shouldLookForECSTasksWhenClustersSet() throws Exception {
            // Given
            properties.set(COMPACTION_CLUSTER, "test-compaction-cluster");
            properties.set(SPLITTING_COMPACTION_CLUSTER, "test-splitting-compaction-cluster");
            List<String> extraECSClusters = List.of("test-system-test-cluster");

            stubFor(post("/")
                    .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                    .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[]}")));

            // When
            shutdownWithExtraEcsClusters(extraECSClusters);

            // Then
            verify(5, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
            verify(1, listTasksRequestedFor("test-ingest-cluster"));
            verify(1, listTasksRequestedFor("test-compaction-cluster"));
            verify(1, listTasksRequestedFor("test-splitting-compaction-cluster"));
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
            verify(3, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
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
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));
        }

        @Test
        void shouldTerminateEMRClusterWhenOneClusterIsRunning() throws Exception {
            // Given
            stubFor(listActiveClustersRequest().inScenario("TerminateEMRClusters")
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
            stubFor(listActiveClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponse().withStatus(200).withBody(
                            "{\"Clusters\": []}"))
                    .whenScenarioStateIs("TERMINATED"));

            // When
            shutdown();

            // Then
            verify(3, postRequestedFor(urlEqualTo("/")));
            verify(2, listActiveClustersRequested());
            verify(1, terminateJobFlowsRequestedFor("test-cluster-id"));
        }

        @Test
        void shouldTerminateEMRClustersInBatchesOfTen() throws Exception {
            // Given
            stubFor(listActiveClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponseWithNumRunningClusters(11))
                    .whenScenarioStateIs(STARTED));
            stubFor(terminateJobFlowsRequestWithJobIdCount(10));
            stubFor(terminateJobFlowsRequestWithJobIdCount(1).inScenario("TerminateEMRClusters")
                    .willSetStateTo("TERMINATED"));
            stubFor(listActiveClustersRequest().inScenario("TerminateEMRClusters")
                    .willReturn(aResponse().withStatus(200).withBody(
                            "{\"Clusters\": []}"))
                    .whenScenarioStateIs("TERMINATED"));

            // When
            shutdown();

            // Then
            verify(4, postRequestedFor(urlEqualTo("/")));
            verify(2, listActiveClustersRequested());

            verify(terminateJobFlowsRequestedWithJobIdsCount(10));
            verify(terminateJobFlowsRequestedWithJobIdsCount(1));
        }

        @Test
        void shouldNotTerminateEMRClusterWhenClusterIsTerminated() throws Exception {
            // Given
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponse().withStatus(200).withBody("" +
                            "{\"Clusters\": []}")));

            // When
            shutdown();

            // Then
            verify(1, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
        }

        @Test
        void shouldNotTerminateEMRClusterWhenClusterBelongsToAnotherInstance() throws Exception {
            // Given
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponse().withStatus(200).withBody("" +
                            "{\"Clusters\": [{" +
                            "   \"Name\": \"sleeper-another-instance-test-cluster\"," +
                            "   \"Id\": \"test-cluster-id\"," +
                            "   \"Status\": {\"State\": \"RUNNING\"}" +
                            "}]}")));

            // When
            shutdown();

            // Then
            verify(1, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
        }
    }


    @Nested
    @DisplayName("Terminate running EMR Serverless Applications")
    class TerminateEMRServerlessApplications {

        @BeforeEach
        void setUp() {
            properties.set(ID, "test-instance");
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponseWithNumRunningClusters(0)));
        }

        @Test
        void shouldAllowEmrServerlessWithNoApplications() throws Exception {
            //Given
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));

            // When
            shutdown();

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldAllowEmrServerlessWithAStoppedApplication() throws Exception {
            //Given
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(List.of(ApplicationState.STOPPED))));

            // When
            shutdown();

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldAllowEmrServerlessWithACreatedApplication() throws Exception {
            //Given
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(List.of(ApplicationState.CREATED))));

            // When
            shutdown();

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithRunningJobs() throws Exception {
            //Given
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(1)));
            stubFor(listJobsForApplicationsRequest("test-application-id-1")
                    .willReturn(aResponseWithNumRunningJobsOnApplication(10, true)));
            stubFor(stopJobForApplicationsRequest("test-application-id-1")
                    .willReturn(aResponseWithNumRunningJobsOnApplication(0)));
            stubFor(deleteJobsForApplicationsRequest("test-application-id-1", "test-job-run-id-1")
                    .willReturn(ResponseDefinitionBuilder.okForEmptyJson()));

            // When
            shutdown();

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithOnlySuccessJobs() throws Exception {
            //Given
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(1)));
            stubFor(listJobsForApplicationsRequest("test-application-id-1")
                    .willReturn(aResponseWithNumRunningJobsOnApplication(1, true)));
            stubFor(stopJobForApplicationsRequest("test-application-id-1")
                    .willReturn(aResponseWithNumRunningJobsOnApplication(0)));
            stubFor(deleteJobsForApplicationsRequest("test-application-id-1", "test-job-run-id-1")
                    .willReturn(ResponseDefinitionBuilder.okForEmptyJson()));

            // When
            shutdown();

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithNoJobs() throws Exception {
            //Given
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(1)));
            stubFor(listJobsForApplicationsRequest("test-application-id-1")
                    .willReturn(aResponseWithNumRunningJobsOnApplication(0)));
            stubFor(stopJobForApplicationsRequest("test-application-id-1")
                    .willReturn(aResponseWithNumRunningJobsOnApplication(0)));

            // When
            shutdown();

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }
    }

    private RequestPatternBuilder listTasksRequestedFor(String clusterName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName)));
    }

    private RequestPatternBuilder stopTaskRequestedFor(String clusterName, String taskArn) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName))
                        .and(matchingJsonPath("$.task", equalTo(taskArn))));
    }
}
