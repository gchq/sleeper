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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.task.common.RunCompactionTasks.HostScaler;
import sleeper.task.common.RunCompactionTasks.TaskCounts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.task.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

public class RunCompactionTasksTest {
    private static final String TEST_JOB_QUEUE = "test-job-queue";
    private static final String TEST_AUTO_SCALING_GROUP = "test-scaling-group";
    private final InstanceProperties instanceProperties = createInstance();
    private final List<Integer> scaleHostsRequests = new ArrayList<>();
    private final HostScaler scaler = scaleHostsRequests::add;

    @DisplayName("Launch tasks using queue")
    @Nested
    class LaunchTasksUsingQueue {
        @Test
        void shouldCreateNoTasksWhenQueueIsEmpty() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            int tasksCreated = runTasks(noMessagesOnQueue(), noExistingTasks());

            // Then
            assertThat(tasksCreated).isZero();
        }

        @Test
        void shouldCreateTaskWhenJobsOnQueueLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            int tasksCreated = runTasks(jobsOnQueue(1), noExistingTasks());

            // Then
            assertThat(tasksCreated).isEqualTo(1);
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueuePlusExistingTasksLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            int tasksCreated = runTasks(jobsOnQueue(1), existingTasks(1));

            // Then
            assertThat(tasksCreated).isEqualTo(1);
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueueEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 1);

            // When
            int tasksCreated = runTasks(jobsOnQueue(1), noExistingTasks());

            // Then
            assertThat(tasksCreated).isEqualTo(1);
        }

        @Test
        void shouldLimitTasksCreatedWhenExistingTasksEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 1);

            // When
            int tasksCreated = runTasks(jobsOnQueue(1), existingTasks(1));

            // Then
            assertThat(tasksCreated).isZero();
        }

        @Test
        void shouldLimitTasksCreatedWhenJobsOnQueueGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 1);

            // When
            int tasksCreated = runTasks(jobsOnQueue(2), noExistingTasks());

            // Then
            assertThat(tasksCreated).isEqualTo(1);
        }

        @Test
        void shouldLimitTasksCreatedWhenExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 1);

            // When
            int tasksCreated = runTasks(jobsOnQueue(1), existingTasks(2));

            // Then
            assertThat(tasksCreated).isZero();
        }

        @Test
        void shouldLimitTasksCreatedWhenJobsOnQueuePlusExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            int tasksCreated = runTasks(jobsOnQueue(2), existingTasks(1));

            // Then
            assertThat(tasksCreated).isEqualTo(1);
        }
    }

    @DisplayName("Launch tasks with target number")
    @Nested
    class LaunchTasksWithTarget {
        @Test
        void shouldCreateTasksWithNoRunningOrPendingTasks() {
            // When
            int tasksCreated = runToMeetTargetTasks(1, noExistingTasks());

            // Then
            assertThat(tasksCreated).isOne();
        }

        @Test
        void shouldCreateTasksOverMaximumConcurrentTasks() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 1);

            // When
            int tasksCreated = runToMeetTargetTasks(2, noExistingTasks());

            // Then
            assertThat(tasksCreated).isEqualTo(2);
        }

        @Test
        void shouldCreateTasksToMeetTargetWithExistingTasks() {
            // When
            int tasksCreated = runToMeetTargetTasks(5, existingTasks(2));

            // Then
            assertThat(tasksCreated).isEqualTo(3);
        }

        @Test
        void shouldDoNothingWhenExistingTasksAreOverTarget() {
            // When
            int tasksCreated = runToMeetTargetTasks(2, existingTasks(3));

            // Then
            assertThat(tasksCreated).isEqualTo(0);
        }

        @Test
        void shouldDoNothingWhenExistingTasksAreEqualToTarget() {
            // When
            int tasksCreated = runToMeetTargetTasks(2, existingTasks(2));

            // Then
            assertThat(tasksCreated).isEqualTo(0);
        }
    }

    @DisplayName("Auto scale if needed")
    @Nested
    class AutoScale {

        @Test
        void shouldNotAutoScaleIfLaunchTypeIsFargate() {
            // Given
            instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "FARGATE");

            // When
            runTasks(jobsOnQueue(5), noExistingTasks());

            // Then
            assertThat(scaleHostsRequests).isEmpty();
        }

        @Test
        void shouldAutoScaleWithNoRunningOrPendingTasks() {
            // Given
            instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");

            // When
            runTasks(jobsOnQueue(5), noExistingTasks());

            // Then
            assertThat(scaleHostsRequests).containsExactly(5);
        }

        @Test
        void shouldAutoScaleWithExistingRunningOrPendingTasks() {
            // Given
            instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");

            // When
            runTasks(jobsOnQueue(5), existingTasks(3));

            // Then
            assertThat(scaleHostsRequests).containsExactly(8);
        }
    }

    private int runTasks(QueueMessageCount.Client queueMessageClient, TaskCounts taskCounts) {
        return run(taskCounts, runTasks -> runTasks.run(queueMessageClient));
    }

    private int runToMeetTargetTasks(int requestedTasks, TaskCounts taskCounts) {
        return run(taskCounts, runTasks -> runTasks.runToMeetTargetTasks(requestedTasks));
    }

    private int run(TaskCounts taskCounts, Consumer<RunCompactionTasks> run) {
        AtomicInteger tasksLaunched = new AtomicInteger();
        RunCompactionTasks runTasks = new RunCompactionTasks(instanceProperties, taskCounts, scaler, (startTime, numberOfTasksToCreate) -> {
            tasksLaunched.set(numberOfTasksToCreate);
        });
        run.accept(runTasks);
        return tasksLaunched.get();
    }

    private static TaskCounts noExistingTasks() {
        return existingTasks(0);
    }

    private static TaskCounts existingTasks(int tasks) {
        return clusterName -> tasks;
    }

    private static QueueMessageCount.Client noMessagesOnQueue() {
        return jobsOnQueue(0);
    }

    private static QueueMessageCount.Client jobsOnQueue(int messageCount) {
        return InMemoryQueueMessageCounts.from(
                Map.of(TEST_JOB_QUEUE, approximateNumberVisibleAndNotVisible(messageCount, 0)));
    }

    private static InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, TEST_JOB_QUEUE);
        instanceProperties.set(COMPACTION_AUTO_SCALING_GROUP, TEST_AUTO_SCALING_GROUP);
        return instanceProperties;
    }

}
