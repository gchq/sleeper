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

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ecs.EcsClient;

import sleeper.core.properties.instance.InstanceProperties;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.task.common.EC2Scaler.create;

public class EC2ScalerTest {

    @Test
    void shouldDetermineValueOfTaskForEC2ScalerFromProperties() {

        // Given / When
        EC2Scaler testScaler = new EC2Scaler(null, null, "testGroup",
                "testCluster", 10, 100);
        // Then
        assertThat(testScaler.calculateAvailableClusterContainerCapacity(generateInstanceDetails())).isEqualTo(25);
    }

    @Test
    void shouldReturnDetailsOfCorrectAutoScalingGroup() {
        // Given
        AmazonAutoScaling asClient = createTestAsClient();

        // When
        AutoScalingGroup group = EC2Scaler.getAutoScalingGroupInfo("autoGroupName", asClient);

        // Then
        assertThat(group.getAutoScalingGroupName()).isEqualTo("autoGroupName");
        assertThat(group.getDesiredCapacity().equals(10));
    }

    @Test
    void shouldThrowExceptionWhenMoreThanOneAutoScalingGroupFound() {
        // Given
        AmazonAutoScaling asClient = createTestAsClient();

        // When / Then
        assertThatThrownBy(() -> EC2Scaler.getAutoScalingGroupInfo("failGroupName", asClient))
                .isInstanceOf(IllegalStateException.class);

    }

    @Test
    void shouldScaleToCorrectSizeBaseOfGroup() {
        // Given
        EC2Scaler scaler = generateTestEc2Scaler(createTestAsClient());

        // When
        scaler.scaleTo("scaleGroupName", 10);

        // Then
        // Create assertions

    }

    private EC2Scaler generateTestEc2Scaler(AmazonAutoScaling asClient) {
        AmazonAutoScalingClientBuilder.defaultClient();
        InstanceProperties testProperties = createTestInstanceProperties();
        testProperties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");

        EcsClient ecsClient = createTestEcsClient();
        return create(testProperties, asClient, ecsClient);
    }

    private EcsClient createTestEcsClient() {
        return EcsClient.create();
    }

    private AmazonAutoScaling createTestAsClient() {
        return AmazonAutoScalingClientBuilder.defaultClient();
    }

    private Map<String, EC2InstanceDetails> generateInstanceDetails() {
        HashMap<String, EC2InstanceDetails> outMap = new HashMap<String, EC2InstanceDetails>();
        EC2InstanceDetails details1 = new EC2InstanceDetails("id1", "arn1", Instant.now(), 100, 1000, 100, 1000, 0, 0); // Give 10 total
        EC2InstanceDetails details2 = new EC2InstanceDetails("id2", "arn2", Instant.now(), 200, 1500, 200, 1500, 0, 0); // Give 15 total

        outMap.put("instance1", details1);
        outMap.put("instance2", details2);

        return outMap;
    }
}
