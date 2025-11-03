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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.emr.model.ClusterState;

import sleeper.core.util.PollWithRetries;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.aResponseWithClusterWithState;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.anyRequestedForEmr;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.describeClusterRequest;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.describeClusterRequestedFor;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.terminateJobFlowsRequest;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.terminateJobFlowsRequestedFor;
import static sleeper.cdk.custom.WiremockEmrPersistentTestHelper.wiremockEmrClient;

@WireMockTest
public class AutoStopEmrPersistentClusterLambdaIT {

    private AutoStopEmrPersistentClusterLambda lambda;
    private String clusterId = "test-cluster-id";

    private CloudFormationCustomResourceEvent clusterEvent(
            String clusterId, String event) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType(event)
                .withResourceProperties(Map.of("clusterId", clusterId))
                .build();
    }

    private AutoStopEmrPersistentClusterLambda lambda(WireMockRuntimeInfo runtimeInfo, PollWithRetries poll) {
        return new AutoStopEmrPersistentClusterLambda(wiremockEmrClient(runtimeInfo), poll);
    }

    @Test
    @DisplayName("Test terminating a running cluster")
    void shouldTerminateEMRClusterWhenClusterIsRunning(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(describeClusterRequest()
                .inScenario("TerminateEMRClusters")
                .willReturn(aResponseWithClusterWithState(clusterId, ClusterState.RUNNING))
                .whenScenarioStateIs(STARTED));
        stubFor(terminateJobFlowsRequest()
                .inScenario("TerminateEMRClusters")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("TERMINATED"));
        stubFor(describeClusterRequest()
                .inScenario("TerminateEMRClusters")
                .willReturn(aResponseWithClusterWithState(clusterId, ClusterState.TERMINATED))
                .whenScenarioStateIs("TERMINATED"));

        // When
        lambda.handleEvent(clusterEvent(clusterId, "Delete"), null);

        // Then
        verify(3, anyRequestedForEmr());
        verify(2, describeClusterRequestedFor(clusterId));
        verify(1, terminateJobFlowsRequestedFor(clusterId));
    }

    @Test
    @DisplayName("Test terminating a stopped cluster")
    void shouldTerminateEMRClusterWhenClusterIsStopped(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(describeClusterRequest()
                .inScenario("TerminateEMRClusters")
                .willReturn(aResponseWithClusterWithState(clusterId, ClusterState.TERMINATED))
                .whenScenarioStateIs(STARTED));
        stubFor(terminateJobFlowsRequest()
                .inScenario("TerminateEMRClusters")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("TERMINATED"));
        stubFor(describeClusterRequest()
                .inScenario("TerminateEMRClusters")
                .willReturn(aResponseWithClusterWithState(clusterId, ClusterState.TERMINATED))
                .whenScenarioStateIs("TERMINATED"));

        // When
        lambda.handleEvent(clusterEvent(clusterId, "Delete"), null);

        // Then
        verify(1, anyRequestedForEmr());
        verify(1, describeClusterRequestedFor(clusterId));
        verify(0, terminateJobFlowsRequestedFor(clusterId));
    }

    @Test
    @DisplayName("Test a time out occurs when terminating a running cluster")
    void shouldTimeOutWhenTerminatingEMRCluster(WireMockRuntimeInfo runtimeInfo) throws Exception {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // Given
        stubFor(describeClusterRequest()
                .inScenario("TerminateEMRClusters")
                .willReturn(aResponseWithClusterWithState(clusterId, ClusterState.RUNNING))
                .whenScenarioStateIs(STARTED));
        stubFor(terminateJobFlowsRequest()
                .inScenario("TerminateEMRClusters")
                .whenScenarioStateIs(STARTED)
                .willSetStateTo("TERMINATING"));
        stubFor(describeClusterRequest()
                .inScenario("TerminateEMRClusters")
                .willReturn(aResponseWithClusterWithState(clusterId, ClusterState.TERMINATING))
                .whenScenarioStateIs("TERMINATING"));

        // When / Then
        assertThatThrownBy(() -> lambda.handleEvent(clusterEvent(clusterId, "Delete"), null))
                .isInstanceOf(PollWithRetries.CheckFailedException.class);
        verify(3, anyRequestedForEmr());
        verify(2, describeClusterRequestedFor(clusterId));
        verify(1, terminateJobFlowsRequestedFor(clusterId));
    }

    @Test
    @DisplayName("Test an invalid event")
    void shouldRaiseExceptionOnInvalidEvent(WireMockRuntimeInfo runtimeInfo) {

        lambda = lambda(runtimeInfo, PollWithRetries.noRetries());

        // When / Then
        assertThatThrownBy(
                () -> lambda.handleEvent(
                        clusterEvent(clusterId, "TagResource"), null))
                .isInstanceOf(IllegalArgumentException.class);
        verify(0, anyRequestedForEmr());
    }

}
