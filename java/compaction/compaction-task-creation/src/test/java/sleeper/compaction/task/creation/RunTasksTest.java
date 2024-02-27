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
package sleeper.compaction.task.creation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.compaction.task.creation.RunTasks.TaskCounts;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.job.common.InMemoryQueueMessageCounts;
import sleeper.job.common.QueueMessageCount;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.job.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

public class RunTasksTest {
    private static final String TEST_JOB_QUEUE = "test-job-queue";
    private final InstanceProperties instanceProperties = createInstance();
    private final Map<String, Integer> numContainersByScalingGroup = new HashMap<>();
    private final Scaler scaler = numContainersByScalingGroup::put;

    @DisplayName("Launch tasks from queue")
    @Nested
    class LaunchFromQueue {
        @Test
        void shouldCreateNoTasksWhenQueueIsEmpty() {
            // When
            int tasksCreated = runTasks(noMessagesOnQueue(), noRunningOrPendingTasks());

            // Then
            assertThat(tasksCreated).isZero();
        }

        @Test
        void shouldCreateOneTasksWhenQueueHasOneMessage() {
            // When
            int tasksCreated = runTasks(messagesOnQueue(1), noRunningOrPendingTasks());

            // Then
            assertThat(tasksCreated).isOne();
        }
    }

    private int runTasks(QueueMessageCount.Client queueMessageClient, TaskCounts taskCounts) {
        AtomicInteger tasksLaunched = new AtomicInteger();
        RunTasks runTasks = new RunTasks(instanceProperties, queueMessageClient, taskCounts, scaler, (startTime, numberOfTasksToCreate) -> {
            tasksLaunched.set(numberOfTasksToCreate);
        });
        runTasks.run();
        return tasksLaunched.get();
    }

    private static TaskCounts noRunningOrPendingTasks() {
        return clusterName -> 0;
    }

    private static QueueMessageCount.Client noMessagesOnQueue() {
        return messagesOnQueue(0);
    }

    private static QueueMessageCount.Client messagesOnQueue(int messageCount) {
        return InMemoryQueueMessageCounts.from(
                Map.of(TEST_JOB_QUEUE, approximateNumberVisibleAndNotVisible(messageCount, 0)));
    }

    private static InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, TEST_JOB_QUEUE);
        return instanceProperties;
    }
}
