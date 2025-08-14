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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.notMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.cdk.custom.WiremockEcsTestHelper.MATCHING_CREATE_CLUSTER_OPERATION;
import static sleeper.cdk.custom.WiremockEcsTestHelper.MATCHING_LIST_TASKS_OPERATION;
import static sleeper.cdk.custom.WiremockEcsTestHelper.MATCHING_STOP_TASK_OPERATION;
import static sleeper.cdk.custom.WiremockEcsTestHelper.MATCHING_UPDATE_CLUSTER_OPERATION;
import static sleeper.cdk.custom.WiremockEcsTestHelper.OPERATION_HEADER;
import static sleeper.cdk.custom.WiremockEcsTestHelper.anyRequestedForEcs;
import static sleeper.cdk.custom.WiremockEcsTestHelper.createClusterRequestedFor;
import static sleeper.cdk.custom.WiremockEcsTestHelper.stopTaskRequestedFor;
import static sleeper.cdk.custom.WiremockEcsTestHelper.updateClusterRequestedFor;
import static sleeper.cdk.custom.WiremockEcsTestHelper.wiremockEcsClient;
import static sleeper.core.util.ThreadSleepTestHelper.noWaits;

@WireMockTest
public class AutoStopEcsClusterTasksLambdaIT {

    private AutoStopEcsClusterTasksLambda lambda;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        lambda = lambda(runtimeInfo);
    }

    @Test
    @DisplayName("Stop tasks on ECS Cluster")
    void shouldStopTasksOnEcsCluster() {

        // Given
        String clusterName = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody("{\"nextToken\":null,\"taskArns\":[\"test-task\"]}")));
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForCluster(clusterName, "Delete"), null);

        // Then
        verify(2, anyRequestedForEcs());
        verify(1, stopTaskRequestedFor(clusterName, "test-task"));
    }

    @Test
    @DisplayName("Stop paged tasks on ECS Cluster")
    void shouldStopPagedTasksOnEcsCluster() {

        // Given
        String clusterName = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                        .withRequestBody(notMatching(".*nexttoken.*"))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(
                                                "{\"nextToken\":\"NEXT_TOKEN\",\"taskArns\":[\"test-task-1\"]}")));
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                        .withRequestBody(matchingJsonPath("$.nextToken", equalTo("NEXT_TOKEN")))
                        .willReturn(
                                aResponse()
                                        .withStatus(200)
                                        .withBody(
                                                "{\"nextToken\":null,\"taskArns\":[\"test-task-2\"]}")));

        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForCluster(clusterName, "Delete"), null);

        // Then
        verify(4, anyRequestedForEcs());
        verify(1, stopTaskRequestedFor(clusterName, "test-task-1"));
        verify(1, stopTaskRequestedFor(clusterName, "test-task-2"));
    }

    @Test
    @DisplayName("Create ECS Cluster")
    void shouldTakeNoActionOnCreateEcsCluster() {

        // Given
        String clusterName = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_CREATE_CLUSTER_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForCluster(clusterName, "Create"), null);

        // Then
        verify(0, createClusterRequestedFor(clusterName));
    }

    @Test
    @DisplayName("Update ECS Cluster")
    void shouldTakeNoActionOnUpdateEcsCluster() {

        // Given
        String clusterName = UUID.randomUUID().toString();
        stubFor(
                post("/")
                        .withHeader(OPERATION_HEADER, MATCHING_UPDATE_CLUSTER_OPERATION)
                        .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForCluster(clusterName, "Update"), null);

        // Then
        verify(0, updateClusterRequestedFor(clusterName));
    }

    @Test
    @DisplayName("Test unsupported operation")
    void shouldRaiseExceptionOnUnsupportedOperation() {

        // Given
        String clusterName = UUID.randomUUID().toString();

        // When / Then
        verify(0, anyRequestedForEcs());
        assertThatThrownBy(
                () -> lambda.handleEvent(
                        eventHandlerForCluster(clusterName, "TagResource"), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private CloudFormationCustomResourceEvent eventHandlerForCluster(
            String clusterName, String event) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType(event)
                .withResourceProperties(Map.of("cluster", clusterName))
                .build();
    }

    private AutoStopEcsClusterTasksLambda lambda(WireMockRuntimeInfo runtimeInfo) {
        return new AutoStopEcsClusterTasksLambda(wiremockEcsClient(runtimeInfo), noWaits(), 10);
    }
}
