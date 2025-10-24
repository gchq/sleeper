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

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import software.amazon.awssdk.services.ecs.EcsClient;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

/**
 * Helper methods to mock an ECS cluster.
 */
public class WiremockEcsTestHelper {

    public static final String OPERATION_HEADER = "X-Amz-Target";
    public static final StringValuePattern MATCHING_LIST_CONTAINERS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListContainerInstances");
    public static final StringValuePattern MATCHING_LIST_TASKS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListTasks");
    public static final StringValuePattern MATCHING_STOP_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.StopTask");
    public static final StringValuePattern MATCHING_CREATE_CLUSTER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.CreateCluster");
    public static final StringValuePattern MATCHING_UPDATE_CLUSTER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.UpdateCluster");
    public static final StringValuePattern MATCHING_DEREGISTER_CONTAINER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeregisterContainerInstance");
    public static final StringValuePattern MATCHING_DELETE_CLUSTER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeleteCluster");
    public static final StringValuePattern MATCHING_TAG_RESOURCE_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.TagResource");

    private WiremockEcsTestHelper() {
    }

    /**
     * Creates a mocked ECS client.
     *
     * @param  runtimeInfo wire mocks runtime info
     * @return             the ECS client
     */
    public static EcsClient wiremockEcsClient(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, EcsClient.builder());
    }

    /**
     * Checks for an ECS stop task request.
     *
     * @param  clusterName the ECS cluster
     * @param  taskArn     the task on the cluster
     * @return             matching HTTP requests
     */
    public static RequestPatternBuilder stopTaskRequestedFor(String clusterName, String taskArn) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_STOP_TASK_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName))
                        .and(matchingJsonPath("$.task", equalTo(taskArn))));
    }

    /**
     * Checks for an ECS create cluster request.
     *
     * @param  clusterName the ECS cluster
     * @return             matching HTTP requests
     */
    public static RequestPatternBuilder createClusterRequestedFor(String clusterName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_CREATE_CLUSTER_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName)));
    }

    /**
     * Checks for an ECS update cluster request.
     *
     * @param  clusterName the ECS cluster
     * @return             matching HTTP requests
     */
    public static RequestPatternBuilder updateClusterRequestedFor(String clusterName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_UPDATE_CLUSTER_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName)));
    }

    /**
     * Checks for any request that matches the pattern.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder anyRequestedForEcs() {
        return anyRequestedFor(anyUrl())
                .withHeader(OPERATION_HEADER, matching("^AmazonEC2ContainerServiceV\\d+\\..*"));
    }

}
