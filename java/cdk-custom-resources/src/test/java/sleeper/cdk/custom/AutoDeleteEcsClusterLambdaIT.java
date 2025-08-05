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
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.cdk.custom.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
public class AutoDeleteEcsClusterLambdaIT {

    private static final String OPERATION_HEADER = "X-Amz-Target";
    public static final StringValuePattern MATCHING_LIST_CONTAINERS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListContainerInstances");
    public static final StringValuePattern MATCHING_LIST_TASKS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListTasks");
    public static final StringValuePattern MATCHING_STOP_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.StopTask");
    private static final StringValuePattern MATCHING_DEREGISTER_CONTAINER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeregisterContainerInstance");
    private static final StringValuePattern MATCHING_DELETE_CLUSTER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeleteCluster");
    private AutoDeleteEcsClusterLambda lambda;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        lambda = lambda(runtimeInfo);
    }

    @Test
    @DisplayName("Shutdown ECS Cluster")
    void shouldDeleteEcsCluster() {

        // Given
        String clusterName = UUID.randomUUID().toString();
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_CONTAINERS_OPERATION)
                .willReturn(aResponse().withStatus(200).withBody("{\"containerInstanceArns\":[\"test-container-arn\"],\"nextToken\":null}")));
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[\"test-task\"]}")));
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                .willReturn(aResponse().withStatus(200)));
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DEREGISTER_CONTAINER_OPERATION)
                .willReturn(aResponse().withStatus(200)));
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DELETE_CLUSTER_OPERATION)
                .willReturn(aResponse().withStatus(200)));

        //When
        lambda.handleEvent(deleteEventForCluster(clusterName), null);

        //Then
        verify(5, anyRequestedForEcs());
        verify(1, stopTaskRequestedFor(clusterName, "test-task"));
        verify(1, deregisterContainerRequestedFor(clusterName, "test-container-arn"));
        verify(1, deleteClusterRequestedFor(clusterName));
    }

    private CloudFormationCustomResourceEvent deleteEventForCluster(String clusterName) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(Map.of("cluster", clusterName))
                .build();
    }

    private AutoDeleteEcsClusterLambda lambda(WireMockRuntimeInfo runtimeInfo) {
        return new AutoDeleteEcsClusterLambda(wiremockEcsClient(runtimeInfo));
    }

    public static RequestPatternBuilder stopTaskRequestedFor(String clusterName, String taskArn) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName))
                        .and(matchingJsonPath("$.task", equalTo(taskArn))));
    }

    public static RequestPatternBuilder deregisterContainerRequestedFor(String clusterName, String containerArn) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DEREGISTER_CONTAINER_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName))
                        .and(matchingJsonPath("$.containerInstance", equalTo(containerArn))));
    }

    public static RequestPatternBuilder deleteClusterRequestedFor(String clusterName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DELETE_CLUSTER_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName)));
    }

    public static RequestPatternBuilder anyRequestedForEcs() {
        return anyRequestedFor(anyUrl())
                .withHeader(OPERATION_HEADER, matching("^AmazonEC2ContainerServiceV\\d+\\..*"));
    }

}
