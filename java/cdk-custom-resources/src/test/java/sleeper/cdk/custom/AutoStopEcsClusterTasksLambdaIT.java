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
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.cdk.custom.WiremockEcsTestHelper.MATCHING_LIST_TASKS_OPERATION;
import static sleeper.cdk.custom.WiremockEcsTestHelper.MATCHING_STOP_TASK_OPERATION;
import static sleeper.cdk.custom.WiremockEcsTestHelper.OPERATION_HEADER;
import static sleeper.cdk.custom.WiremockEcsTestHelper.anyRequestedForEcs;
import static sleeper.cdk.custom.WiremockEcsTestHelper.stopTaskRequestedFor;
import static sleeper.cdk.custom.WiremockEcsTestHelper.wiremockEcsClient;

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
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
                .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[\"test-task\"]}")));
        stubFor(post("/")
                .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                .willReturn(aResponse().withStatus(200)));

        //When
        lambda.handleEvent(deleteEventForCluster(clusterName), null);

        //Then
        verify(2, anyRequestedForEcs());
        verify(1, stopTaskRequestedFor(clusterName, "test-task"));
    }

    private CloudFormationCustomResourceEvent deleteEventForCluster(String clusterName) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(Map.of("cluster", clusterName))
                .build();
    }

    private AutoStopEcsClusterTasksLambda lambda(WireMockRuntimeInfo runtimeInfo) {
        return new AutoStopEcsClusterTasksLambda(wiremockEcsClient(runtimeInfo));
    }

}
