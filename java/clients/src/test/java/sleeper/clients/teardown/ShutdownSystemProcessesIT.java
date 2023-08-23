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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

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
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrServerlessClient;
import static sleeper.clients.testutil.WiremockEMRTestHelper.OPERATION_HEADER;
import static sleeper.clients.testutil.WiremockEMRTestHelper.aResponseWithNumRunningApplications;
import static sleeper.clients.testutil.WiremockEMRTestHelper.aResponseWithNumRunningClusters;
import static sleeper.clients.testutil.WiremockEMRTestHelper.aResponseWithNumRunningJobsOnApplication;
import static sleeper.clients.testutil.WiremockEMRTestHelper.deleteJobsForApplicationsRequest;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listActiveApplicationRequested;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listActiveApplicationsRequest;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listActiveClustersRequest;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listActiveClustersRequested;
import static sleeper.clients.testutil.WiremockEMRTestHelper.listJobsForApplicationsRequest;
import static sleeper.clients.testutil.WiremockEMRTestHelper.stopJobForApplicationsRequest;
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

    private static final StringValuePattern MATCHING_DISABLE_RULE_OPERATION = matching("^AWSEvents\\.DisableRule$");
    private static final StringValuePattern MATCHING_LIST_TASKS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListTasks");
    private static final StringValuePattern MATCHING_STOP_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.StopTask");
    private static final StringValuePattern MATCHING_TERMINATE_JOB_FLOWS_OPERATION = matching("ElasticMapReduce.TerminateJobFlows");

    private ShutdownSystemProcesses shutdown;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        shutdown = new ShutdownSystemProcesses(wiremockCloudWatchClient(runtimeInfo), wiremockEcsClient(runtimeInfo),
                wiremockEmrClient(runtimeInfo), wiremockEmrServerlessClient(runtimeInfo));
    }

    private void shutdown(InstanceProperties instanceProperties) throws Exception {
        shutdown.shutdown(instanceProperties, List.of());
    }

    private void shutdown(InstanceProperties instanceProperties, List<String> extraECSClusters) throws Exception {
        shutdown.shutdown(instanceProperties, extraECSClusters);
    }

    @Test
    void shouldShutDownCloudWatchRulesWhenSet() throws Exception {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-compaction-job-creation-rule");
        properties.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-compaction-task-creation-rule");
        properties.set(SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-splitting-compaction-task-creation-rule");
        properties.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, "test-partition-splitting-rule");
        properties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-garbage-collector-rule");
        properties.set(INGEST_CLOUDWATCH_RULE, "test-ingest-task-creation-rule");
        properties.set(TABLE_METRICS_RULES, "test-table-metrics-rule-1,test-table-metrics-rule-2");

        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DISABLE_RULE_OPERATION)
                .willReturn(aResponse().withStatus(200)));
        stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
        stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(0)));

        // When
        shutdown(properties);

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

    @Test
    void shouldLookForECSTasksWhenClustersSet() throws Exception {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(INGEST_CLUSTER, "test-ingest-cluster");
        properties.set(COMPACTION_CLUSTER, "test-compaction-cluster");
        properties.set(SPLITTING_COMPACTION_CLUSTER, "test-splitting-compaction-cluster");
        List<String> extraECSClusters = List.of("test-system-test-cluster");

        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[]}")));
        stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
        stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(0)));

        // When
        shutdown(properties, extraECSClusters);

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
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(INGEST_CLUSTER, "test-ingest-cluster");

        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[\"test-task\"]}")));
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                .willReturn(aResponse().withStatus(200)));
        stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
        stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(0)));

        // When
        shutdown(properties);

        // Then
        verify(3, postRequestedFor(urlEqualTo("/")));
        verify(1, listActiveClustersRequested());
        verify(1, listTasksRequestedFor("test-ingest-cluster"));
        verify(1, stopTaskRequestedFor("test-ingest-cluster", "test-task"));
    }

    @Nested
    @DisplayName("Terminate running EMR clusters")
    class TerminateEMRClusters {

        @Test
        void shouldTerminateEMRClusterWhenOneClusterIsRunning() throws Exception {
            // Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
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
            stubFor(listActiveApplicationsRequest()
                   .willReturn(aResponseWithNumRunningApplications(0)));

            // When
            shutdown(properties);

            // Then
            verify(3, postRequestedFor(urlEqualTo("/")));
            verify(2, listActiveClustersRequested());
            verify(1, terminateJobFlowsRequestedFor("test-cluster-id"));
        }

        @Test
        void shouldTerminateEMRClustersInBatchesOfTen() throws Exception {
            // Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
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
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));

            // When
            shutdown(properties);

            // Then
            verify(4, postRequestedFor(urlEqualTo("/")));
            verify(2, listActiveClustersRequested());

            verify(terminateJobFlowsRequestedWithJobIdsCount(10));
            verify(terminateJobFlowsRequestedWithJobIdsCount(1));
        }

        @Test
        void shouldNotTerminateEMRClusterWhenClusterIsTerminated() throws Exception {
            // Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponse().withStatus(200).withBody("" +
                            "{\"Clusters\": []}")));
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));

            // When
            shutdown(properties);

            // Then
            verify(1, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
        }

        @Test
        void shouldNotTerminateEMRClusterWhenClusterBelongsToAnotherInstance() throws Exception {
            // Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveApplicationsRequest()
                    .willReturn(aResponseWithNumRunningApplications(0)));
            stubFor(listActiveClustersRequest()
                    .willReturn(aResponse().withStatus(200).withBody("" +
                            "{\"Clusters\": [{" +
                            "   \"Name\": \"sleeper-another-instance-test-cluster\"," +
                            "   \"Id\": \"test-cluster-id\"," +
                            "   \"Status\": {\"State\": \"RUNNING\"}" +
                            "}]}")));

            // When
            shutdown(properties);

            // Then
            verify(1, postRequestedFor(urlEqualTo("/")));
            verify(1, listActiveClustersRequested());
        }
    }


    @Nested
    @DisplayName("Terminate running EMR Serverless Applications")
    class TerminateEMRServerlessApplications {

        @Test
        void shouldAllowEmrServerlessWithNoApplications() throws Exception {
            //Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(0)));

            // When
            shutdown(properties);

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldAllowEmrServerlessWithAStoppedApplication() throws Exception {
            //Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(1, true)));

            // When
            shutdown(properties);

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithRunningJobs() throws Exception {
            //Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(1)));
            stubFor(listJobsForApplicationsRequest("test-application-id-1")
                .willReturn(aResponseWithNumRunningJobsOnApplication(10, true)));
            stubFor(stopJobForApplicationsRequest("test-application-id-1")
                .willReturn(aResponseWithNumRunningJobsOnApplication(0)));
            stubFor(deleteJobsForApplicationsRequest("test-application-id-1", "test-job-run-id-1")
                .willReturn(ResponseDefinitionBuilder.okForEmptyJson()));

            // When
            shutdown(properties);

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithOnlySuccessJobs() throws Exception {
            //Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(1)));
            stubFor(listJobsForApplicationsRequest("test-application-id-1")
                .willReturn(aResponseWithNumRunningJobsOnApplication(1, true)));
            stubFor(stopJobForApplicationsRequest("test-application-id-1")
                .willReturn(aResponseWithNumRunningJobsOnApplication(0)));
            stubFor(deleteJobsForApplicationsRequest("test-application-id-1", "test-job-run-id-1")
                .willReturn(ResponseDefinitionBuilder.okForEmptyJson()));

            // When
            shutdown(properties);

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }

        @Test
        void shouldStopEMRServerlessWhenApplicationIsStartedWithNoJobs() throws Exception {
            //Given
            InstanceProperties properties = createTestInstancePropertiesWithEmrStack();
            stubFor(listActiveClustersRequest()
                .willReturn(aResponseWithNumRunningClusters(0)));
            stubFor(listActiveApplicationsRequest()
                .willReturn(aResponseWithNumRunningApplications(1)));
            stubFor(listJobsForApplicationsRequest("test-application-id-1")
                .willReturn(aResponseWithNumRunningJobsOnApplication(0)));
            stubFor(stopJobForApplicationsRequest("test-application-id-1")
                .willReturn(aResponseWithNumRunningJobsOnApplication(0)));

            // When
            shutdown(properties);

            // Then
            verify(1, getRequestedFor(urlEqualTo("/applications")));
            verify(1, listActiveApplicationRequested());
        }
    }

    private MappingBuilder terminateJobFlowsRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION)
                .willReturn(aResponse().withStatus(200));
    }

    private MappingBuilder terminateJobFlowsRequestWithJobIdCount(int jobIdsCount) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION)
                .withRequestBody(matchingJsonPath("$.JobFlowIds.size()",
                        equalTo(jobIdsCount + "")))
                .willReturn(aResponse().withStatus(200));
    }


    private RequestPatternBuilder terminateJobFlowsRequested() {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_TERMINATE_JOB_FLOWS_OPERATION);
    }

    private RequestPatternBuilder terminateJobFlowsRequestedFor(String clusterId) {
        return terminateJobFlowsRequested()
                .withRequestBody(matchingJsonPath("$.JobFlowIds",
                        equalTo(clusterId)));
    }

    private RequestPatternBuilder terminateJobFlowsRequestedWithJobIdsCount(int jobIdsCount) {
        return terminateJobFlowsRequested()
                .withRequestBody(matchingJsonPath("$.JobFlowIds.size()",
                        equalTo(jobIdsCount + "")));
    }

    private InstanceProperties createTestInstancePropertiesWithEmrStack() {
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(ID, "test-instance");
        return properties;
    }

    private RequestPatternBuilder disableRuleRequestedFor(String ruleName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DISABLE_RULE_OPERATION)
                .withRequestBody(matchingJsonPath("$.Name", equalTo(ruleName)));
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
