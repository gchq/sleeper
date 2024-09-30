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
package sleeper.task.common;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecs.EcsClient;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.task.common.WiremockTestHelper.wiremockEcsClient;

@WireMockTest
public class EC2InstanceDetailsIT {

    private static final String OPERATION_HEADER = "X-Amz-Target";
    private static final StringValuePattern MATCHING_LIST_INSTANCES_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListContainerInstances");
    private static final StringValuePattern MATCHING_DESCRIBE_INSTANCES_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.DescribeContainerInstances");

    private EcsClient ecsClient;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        ecsClient = wiremockEcsClient(runtimeInfo);
    }

    @Test
    void shouldDescribeNoInstances() {
        stubFor(listActiveInstancesForCluster("test-cluster")
                .willReturn(aResponse().withStatus(200)
                        .withBody("{\"containerInstanceArns\": []}")));

        assertThat(EC2InstanceDetails.fetchInstanceDetails("test-cluster", ecsClient))
                .isEmpty();
    }

    @Test
    void shouldDescribeOneInstance() {
        stubFor(listActiveInstancesForCluster("test-cluster")
                .willReturn(aResponse().withStatus(200)
                        .withBody("{\"containerInstanceArns\": [\"test-instance-arn\"]}")));
        stubFor(describeClusterInstances("test-cluster", "test-instance-arn")
                .willReturn(aResponse().withStatus(200)
                        .withBody("{\"containerInstances\": [{" +
                                "\"containerInstanceArn\": \"test-instance-arn\"," +
                                "\"ec2InstanceId\": \"test-instance-id\"," +
                                "\"registeredAt\": 1727697362," +
                                "\"remainingResources\": [{\"name\":\"CPU\",\"type\":\"INTEGER\",\"integerValue\":2},{\"name\":\"MEMORY\",\"type\":\"INTEGER\",\"integerValue\":256}]," +
                                "\"registeredResources\": [{\"name\":\"CPU\",\"type\":\"INTEGER\",\"integerValue\":4},{\"name\":\"MEMORY\",\"type\":\"INTEGER\",\"integerValue\":512}]," +
                                "\"runningTasksCount\": 1," +
                                "\"pendingTasksCount\": 1" +
                                "}], \"failures\": []}")));

        assertThat(EC2InstanceDetails.fetchInstanceDetails("test-cluster", ecsClient))
                .isEqualTo(Map.of("test-instance-id",
                        new EC2InstanceDetails("test-instance-id", "test-instance-arn",
                                Instant.parse("2024-09-30T11:56:02Z"), 2, 256, 4, 512, 1, 1)));
    }

    @Test
    void shouldDescribeTwoInstances() {
        stubFor(listActiveInstancesForCluster("some-cluster")
                .willReturn(aResponse().withStatus(200)
                        .withBody("{\"containerInstanceArns\": [\"instance-arn-1\",\"instance-arn-2\"]}")));
        stubFor(describeClusterInstances("some-cluster", "instance-arn-1", "instance-arn-2")
                .willReturn(aResponse().withStatus(200)
                        .withBody("{\"containerInstances\": [{" +
                                "\"containerInstanceArn\": \"instance-arn-1\"," +
                                "\"ec2InstanceId\": \"instance-id-1\"," +
                                "\"registeredAt\": 1727697362," +
                                "\"remainingResources\": [{\"name\":\"CPU\",\"type\":\"INTEGER\",\"integerValue\":2},{\"name\":\"MEMORY\",\"type\":\"INTEGER\",\"integerValue\":256}]," +
                                "\"registeredResources\": [{\"name\":\"CPU\",\"type\":\"INTEGER\",\"integerValue\":4},{\"name\":\"MEMORY\",\"type\":\"INTEGER\",\"integerValue\":512}]," +
                                "\"runningTasksCount\": 1," +
                                "\"pendingTasksCount\": 1" +
                                "},{" +
                                "\"containerInstanceArn\": \"instance-arn-2\"," +
                                "\"ec2InstanceId\": \"instance-id-2\"," +
                                "\"registeredAt\": 1727698272," +
                                "\"remainingResources\": [{\"name\":\"CPU\",\"type\":\"INTEGER\",\"integerValue\":4},{\"name\":\"MEMORY\",\"type\":\"INTEGER\",\"integerValue\":512}]," +
                                "\"registeredResources\": [{\"name\":\"CPU\",\"type\":\"INTEGER\",\"integerValue\":8},{\"name\":\"MEMORY\",\"type\":\"INTEGER\",\"integerValue\":1024}]," +
                                "\"runningTasksCount\": 2," +
                                "\"pendingTasksCount\": 2" +
                                "}], \"failures\": []}")));

        assertThat(EC2InstanceDetails.fetchInstanceDetails("some-cluster", ecsClient))
                .isEqualTo(Map.of(
                        "instance-id-1",
                        new EC2InstanceDetails("instance-id-1", "instance-arn-1",
                                Instant.parse("2024-09-30T11:56:02Z"), 2, 256, 4, 512, 1, 1),
                        "instance-id-2",
                        new EC2InstanceDetails("instance-id-2", "instance-arn-2",
                                Instant.parse("2024-09-30T12:11:12Z"), 4, 512, 8, 1024, 2, 2)));
    }

    private static MappingBuilder listActiveInstancesForCluster(String clusterName) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_LIST_INSTANCES_OPERATION)
                .withRequestBody(equalToJson("{\"cluster\": \"" + clusterName + "\", \"maxResults\": 75, \"status\": \"ACTIVE\"}"));
    }

    private static MappingBuilder describeClusterInstances(String clusterName, String... instanceArns) {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DESCRIBE_INSTANCES_OPERATION)
                .withRequestBody(equalToJson("{\"cluster\": \"" + clusterName + "\", " +
                        "\"containerInstances\": [\"" + Stream.of(instanceArns).collect(joining("\",\"")) + "\"]}"));
    }

}
