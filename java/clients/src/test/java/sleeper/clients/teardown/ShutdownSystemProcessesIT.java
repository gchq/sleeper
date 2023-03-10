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

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.DummyInstanceProperty;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.WiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.WiremockTestHelper.wiremockEcsClient;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_CLUSTER;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.TABLE_METRICS_RULES;

@WireMockTest
class ShutdownSystemProcessesIT {

    private static final String OPERATION_HEADER_KEY = "X-Amz-Target";
    private static final StringValuePattern MATCHING_DISABLE_RULE_OPERATION = matching("^AWSEvents\\.DisableRule$");
    private static final StringValuePattern MATCHING_LIST_TASKS_OPERATION = matching("^AmazonEC2ContainerServiceV\\d+\\.ListTasks");

    private ShutdownSystemProcesses shutdown;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        shutdown = new ShutdownSystemProcesses(wiremockCloudWatchClient(runtimeInfo), wiremockEcsClient(runtimeInfo));
    }

    private void shutdown(InstanceProperties instanceProperties) {
        shutdown.shutdown(instanceProperties, List.of());
    }

    private void shutdown(InstanceProperties instanceProperties, List<InstanceProperty> extraClusters) {
        shutdown.shutdown(instanceProperties, extraClusters);
    }

    @Test
    void shouldShutDownCloudWatchRulesWhenSet() {
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
                .withHeader(OPERATION_HEADER_KEY, MATCHING_DISABLE_RULE_OPERATION)
                .willReturn(aResponse().withStatus(200)));

        // When
        shutdown(properties);

        // Then
        verify(8, postRequestedFor(urlEqualTo("/")));
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
    void shouldLookForECSTasksWhenClustersSet() {
        // Given
        InstanceProperties properties = createTestInstanceProperties();
        properties.set(INGEST_CLUSTER, "test-ingest-cluster");
        properties.set(COMPACTION_CLUSTER, "test-compaction-cluster");
        properties.set(SPLITTING_COMPACTION_CLUSTER, "test-splitting-compaction-cluster");

        InstanceProperty extraClusterProperty = new DummyInstanceProperty("sleeper.systemtest.cluster");
        properties.set(extraClusterProperty, "test-system-test-cluster");

        stubFor(post("/")
                .withHeader(OPERATION_HEADER_KEY, MATCHING_LIST_TASKS_OPERATION)
                .willReturn(aResponse().withStatus(200).withBody("{\"nextToken\":null,\"taskArns\":[]}")));

        // When
        shutdown(properties, List.of(extraClusterProperty));

        // Then
        verify(4, postRequestedFor(urlEqualTo("/")));
        verify(1, listTasksRequestedFor("test-ingest-cluster"));
        verify(1, listTasksRequestedFor("test-compaction-cluster"));
        verify(1, listTasksRequestedFor("test-splitting-compaction-cluster"));
        verify(1, listTasksRequestedFor("test-system-test-cluster"));
    }

    private RequestPatternBuilder disableRuleRequestedFor(String ruleName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER_KEY, MATCHING_DISABLE_RULE_OPERATION)
                .withRequestBody(equalToJson("{\"Name\":\"" + ruleName + "\"}"));
    }

    private RequestPatternBuilder listTasksRequestedFor(String clusterName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER_KEY, MATCHING_LIST_TASKS_OPERATION)
                .withRequestBody(equalToJson("{\"cluster\":\"" + clusterName + "\"}"));
    }

}
