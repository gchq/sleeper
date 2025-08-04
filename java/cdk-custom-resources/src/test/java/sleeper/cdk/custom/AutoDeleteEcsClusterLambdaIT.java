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

import sleeper.core.properties.instance.InstanceProperties;

import java.util.Map;
import java.util.UUID;

import static sleeper.cdk.custom.WiremockTestHelper.wiremockEcsClient;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@WireMockTest
public class AutoDeleteEcsClusterLambdaIT {

    private final InstanceProperties properties = createTestInstanceProperties();
    private AutoDeleteEcsClusterLambda deleteCluster;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        deleteCluster = lambda(runtimeInfo);
    }

    @Test
    @DisplayName("Shutdown ECS Cluster")
    void shouldDeleteEcsCluster() {

        // Given
        properties.set(INGEST_CLUSTER, "test-ingest-cluster");
        String clusterName = UUID.randomUUID().toString();

        //stubFor(post("/")
        //        .withHeader(OPERATION_HEADER, MATCHING_LIST_TASKS_OPERATION)
        //        .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[]}")));

        //When
        deleteCluster.handleEvent(deleteEventForCluster(clusterName), null);
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

}
