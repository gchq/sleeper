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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecs.EcsClient;

import java.net.URI;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class WiremockTestHelper {

    public static final String WIREMOCK_ACCESS_KEY = "wiremock-access-key";
    public static final String WIREMOCK_SECRET_KEY = "wiremock-secret-key";

    public static final String OPERATION_HEADER = "X-Amz-Target";
    public static final StringValuePattern MATCHING_LIST_CONTAINERS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListContainerInstances");
    public static final StringValuePattern MATCHING_LIST_TASKS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListTasks");
    public static final StringValuePattern MATCHING_STOP_TASK_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.StopTask");
    public static final StringValuePattern MATCHING_DEREGISTER_CONTAINER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeregisterContainerInstance");
    public static final StringValuePattern MATCHING_DELETE_CLUSTER_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DeleteCluster");

    private WiremockTestHelper() {
    }

    public static Ec2Client wiremockEc2Client(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, Ec2Client.builder());
    }

    public static <B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, T>, T> T wiremockAwsV2Client(WireMockRuntimeInfo runtimeInfo, B builder) {
        return builder
                .endpointOverride(URI.create(runtimeInfo.getHttpBaseUrl()))
                .region(Region.AWS_GLOBAL)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(WIREMOCK_ACCESS_KEY, WIREMOCK_SECRET_KEY)))
                .build();
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
     * Checks for an ECS de-register container request.
     *
     * @param  clusterName  the ECS cluster
     * @param  containerArn the container on the cluster
     * @return              matching HTTP requests
     */
    public static RequestPatternBuilder deregisterContainerRequestedFor(String clusterName, String containerArn) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DEREGISTER_CONTAINER_OPERATION)
                .withRequestBody(matchingJsonPath("$.cluster", equalTo(clusterName))
                        .and(matchingJsonPath("$.containerInstance", equalTo(containerArn))));
    }

    /**
     * Checks for an ECS delete cluster request.
     *
     * @param  clusterName the ECS cluster
     * @return             matching HTTP requests
     */
    public static RequestPatternBuilder deleteClusterRequestedFor(String clusterName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DELETE_CLUSTER_OPERATION)
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
