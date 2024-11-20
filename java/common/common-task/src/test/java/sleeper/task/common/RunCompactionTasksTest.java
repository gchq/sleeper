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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.task.common.CompactionTaskHostScaler.CheckAutoScalingGroup;
import sleeper.task.common.CompactionTaskHostScaler.CheckInstanceType;
import sleeper.task.common.CompactionTaskHostScaler.InstanceType;
import sleeper.task.common.CompactionTaskHostScaler.SetDesiredInstances;
import sleeper.task.common.RunCompactionTasks.TaskCounts;
import sleeper.task.common.RunCompactionTasks.TaskLauncher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.task.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

public class RunCompactionTasksTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Map<String, InstanceType> instanceTypes = new HashMap<>();
    private final Map<String, FakeAutoScalingGroup> autoScalingGroupByName = new HashMap<>();
    private final List<Integer> launchTasksRequests = new ArrayList<>();
    private final List<Integer> scaleToHostsRequests = new ArrayList<>();

    @BeforeEach
    void setUp() {
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, "test-compaction-job-queue");
        instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");
        instanceProperties.set(COMPACTION_AUTO_SCALING_GROUP, "test-scaling-group");
        setScalingGroupMaxSize(10);
    }

    @DisplayName("Launch tasks using queue")
    @Nested
    class LaunchTasksUsingQueue {
        @Test
        void shouldCreateNoTasksWhenQueueIsEmpty() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(noJobsOnQueue(), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).contains(0);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueueLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(jobsOnQueue(2), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueueEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(2), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueuePlusExistingTasksLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(jobsOnQueue(2), existingTasks(1));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateNoTasksWhenExistingTasksEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(1), existingTasks(2));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldCreateNoTasksWhenExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(1), existingTasks(3));

            // Then
            assertThat(scaleToHostsRequests).contains(2);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldCreateTasksToMaxWhenJobsOnQueueGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(3), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksToMaxWhenJobsOnQueuePlusExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 3);

            // When
            runTasks(jobsOnQueue(2), existingTasks(2));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).containsExactly(1);
        }

        @Test
        void shouldScaleHostsWhenNoJobsOnQueueAndExistingTasksPresentAndLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(noJobsOnQueue(), existingTasks(3));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleHostsWhenNoJobsOnQueueAndExistingTasksEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 3);

            // When
            runTasks(noJobsOnQueue(), existingTasks(3));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleHostsWhenNoJobsOnQueueAndExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(noJobsOnQueue(), existingTasks(3));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).isEmpty();
        }
    }

    @DisplayName("Launch tasks with target number")
    @Nested
    class LaunchTasksWithTarget {
        @Test
        void shouldCreateTasksWithNoExistingTasks() {
            // When
            runToMeetTargetTasks(2, noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksOverMaximumConcurrentTasks() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runToMeetTargetTasks(3, noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldCreateTasksToMeetTargetWithExistingTasks() {
            // When
            runToMeetTargetTasks(5, existingTasks(2));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(5);
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldDoNothingWhenExistingTasksAreOverTarget() {
            // When
            runToMeetTargetTasks(2, existingTasks(3));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldDoNothingWhenExistingTasksAreEqualToTarget() {
            // When
            runToMeetTargetTasks(2, existingTasks(2));

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleToZeroWhenNoExistingTasks() {
            // When
            runToMeetTargetTasks(0, noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(0);
            assertThat(launchTasksRequests).isEmpty();
        }
    }

    private void runTasks(QueueMessageCount.Client queueClient, TaskCounts taskCounts) {
        taskRunner(taskCounts).run(queueClient);
    }

    private void runToMeetTargetTasks(int requestedTasks, TaskCounts taskCounts) {
        taskRunner(taskCounts).runToMeetTargetTasks(requestedTasks);
    }

    private RunCompactionTasks taskRunner(TaskCounts taskCounts) {
        return new RunCompactionTasks(instanceProperties, taskCounts, hostScaler(), taskLauncher());
    }

    private CompactionTaskHostScaler hostScaler() {
        return new CompactionTaskHostScaler(instanceProperties, checkAutoScalingGroup(), setDesiredInstances(), checkInstanceType());
    }

    private TaskLauncher taskLauncher() {
        return (numberOfTasks, checkAbort) -> launchTasksRequests.add(numberOfTasks);
    }

    private CheckAutoScalingGroup checkAutoScalingGroup() {
        return groupName -> autoScalingGroupByName.get(groupName).maxSize();
    }

    private SetDesiredInstances setDesiredInstances() {
        return (groupName, desiredSize) -> {
            autoScalingGroupByName.compute(groupName, (name, group) -> group.withDesiredSize(desiredSize));
            scaleToHostsRequests.add(desiredSize);
        };
    }

    private CheckInstanceType checkInstanceType() {
        return instanceTypes::get;
    }

    private static TaskCounts noExistingTasks() {
        return existingTasks(0);
    }

    private static TaskCounts existingTasks(int tasks) {
        return () -> tasks;
    }

    private QueueMessageCount.Client noJobsOnQueue() {
        return jobsOnQueue(0);
    }

    private QueueMessageCount.Client jobsOnQueue(int messageCount) {
        return InMemoryQueueMessageCounts.from(
                Map.of(instanceProperties.get(COMPACTION_JOB_QUEUE_URL),
                        approximateNumberVisibleAndNotVisible(messageCount, 0)));
    }

    private void setScalingGroupMaxSize(int maxSize) {
        autoScalingGroupByName.compute(instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP),
                (name, group) -> Optional.ofNullable(group)
                        .map(g -> g.withMaxSize(maxSize))
                        .orElseGet(() -> new FakeAutoScalingGroup(maxSize)));
    }

    private record FakeAutoScalingGroup(int maxSize, int desiredSize) {

        FakeAutoScalingGroup(int maxSize) {
            this(maxSize, 0);
        }

        public FakeAutoScalingGroup withMaxSize(int maxSize) {
            return new FakeAutoScalingGroup(maxSize, desiredSize);
        }

        public FakeAutoScalingGroup withDesiredSize(int desiredSize) {
            return new FakeAutoScalingGroup(maxSize, desiredSize);
        }
    }
}
