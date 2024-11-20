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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_AUTO_SCALING_GROUP;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_EC2_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_CPU_ARCHITECTURE;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_X86_CPU;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TASK_X86_MEMORY;
import static sleeper.core.properties.instance.CompactionProperty.MAXIMUM_CONCURRENT_COMPACTION_TASKS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.task.common.QueueMessageCount.approximateNumberVisibleAndNotVisible;

public class RunCompactionTasksTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Map<String, InstanceType> instanceTypes = new HashMap<>();
    private final Map<String, Integer> autoScalingGroupMaxSizeByName = new HashMap<>();
    private final List<Integer> launchTasksRequests = new ArrayList<>();
    private final List<Integer> scaleToHostsRequests = new ArrayList<>();

    @BeforeEach
    void setUp() {
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, "test-compaction-job-queue");
        instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "FARGATE");
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
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueueLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(jobsOnQueue(2), noExistingTasks());

            // Then
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueueEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(2), noExistingTasks());

            // Then
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksWhenJobsOnQueuePlusExistingTasksLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(jobsOnQueue(2), existingTasks(1));

            // Then
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateNoTasksWhenExistingTasksEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(1), existingTasks(2));

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldCreateNoTasksWhenExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(1), existingTasks(3));

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldCreateTasksToMaxWhenJobsOnQueueGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(jobsOnQueue(3), noExistingTasks());

            // Then
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksToMaxWhenJobsOnQueuePlusExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 3);

            // When
            runTasks(jobsOnQueue(2), existingTasks(2));

            // Then
            assertThat(launchTasksRequests).containsExactly(1);
        }

        @Test
        void shouldScaleHostsWhenNoJobsOnQueueAndExistingTasksPresentAndLessThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 5);

            // When
            runTasks(noJobsOnQueue(), existingTasks(3));

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleHostsWhenNoJobsOnQueueAndExistingTasksEqualToMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 3);

            // When
            runTasks(noJobsOnQueue(), existingTasks(3));

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleHostsWhenNoJobsOnQueueAndExistingTasksGreaterThanMax() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runTasks(noJobsOnQueue(), existingTasks(3));

            // Then
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
            assertThat(launchTasksRequests).containsExactly(2);
        }

        @Test
        void shouldCreateTasksOverMaximumConcurrentTasks() {
            // Given
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 2);

            // When
            runToMeetTargetTasks(3, noExistingTasks());

            // Then
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldCreateTasksToMeetTargetWithExistingTasks() {
            // When
            runToMeetTargetTasks(5, existingTasks(2));

            // Then
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldDoNothingWhenExistingTasksAreOverTarget() {
            // When
            runToMeetTargetTasks(2, existingTasks(3));

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldDoNothingWhenExistingTasksAreEqualToTarget() {
            // When
            runToMeetTargetTasks(2, existingTasks(2));

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleToZeroWhenNoExistingTasks() {
            // When
            runToMeetTargetTasks(0, noExistingTasks());

            // Then
            assertThat(launchTasksRequests).isEmpty();
        }
    }

    @Nested
    @DisplayName("Scale hosts to support required tasks")
    class ScaleHostsToTasks {

        @BeforeEach
        void setUp() {
            instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "EC2");
            instanceProperties.set(COMPACTION_AUTO_SCALING_GROUP, "test-scaling-group");
            instanceProperties.setNumber(MAXIMUM_CONCURRENT_COMPACTION_TASKS, 10);
            setScalingGroupMaxSize(10);
        }

        @Test
        void shouldScaleToTargetTasksWhenInstanceTypeIsUnknown() {
            // When
            runTasks(jobsOnQueue(3), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(3);
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldScaleToOneWhenInstanceTypeFitsThreeTasks() {
            // Given
            instanceProperties.set(COMPACTION_EC2_TYPE, "test-type");
            instanceTypes.put("test-type", new InstanceType(4, 4096));
            instanceProperties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");
            instanceProperties.setNumber(COMPACTION_TASK_X86_CPU, 1024);
            instanceProperties.setNumber(COMPACTION_TASK_X86_MEMORY, 1024);

            // When
            runTasks(jobsOnQueue(3), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(1);
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldScaleToTwoWhenInstanceTypeFitsThreeTasks() {
            // Given
            instanceProperties.set(COMPACTION_EC2_TYPE, "test-type");
            instanceTypes.put("test-type", new InstanceType(4, 4096));
            instanceProperties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");
            instanceProperties.setNumber(COMPACTION_TASK_X86_CPU, 1024);
            instanceProperties.setNumber(COMPACTION_TASK_X86_MEMORY, 1024);

            // When
            runTasks(jobsOnQueue(6), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).containsExactly(6);
        }

        @Test
        void shouldScaleToMaxSizeWhenNeededTasksDoNotFit() {
            // Given
            setScalingGroupMaxSize(2);

            // When
            runTasks(jobsOnQueue(3), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(2);
            assertThat(launchTasksRequests).containsExactly(3);
        }

        @Test
        void shouldFailWhenInstanceTypeDoesNotFitOneTask() {
            // Given
            instanceProperties.set(COMPACTION_EC2_TYPE, "test-type");
            instanceTypes.put("test-type", new InstanceType(1, 1024));
            instanceProperties.set(COMPACTION_TASK_CPU_ARCHITECTURE, "X86_64");
            instanceProperties.setNumber(COMPACTION_TASK_X86_CPU, 1024);
            instanceProperties.setNumber(COMPACTION_TASK_X86_MEMORY, 1024);

            // When / Then
            assertThatThrownBy(() -> runTasks(jobsOnQueue(5), noExistingTasks()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(scaleToHostsRequests).isEmpty();
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldScaleToZeroWhenNoTasksArePresentOrNeeded() {
            // When
            runTasks(noJobsOnQueue(), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).containsExactly(0);
            assertThat(launchTasksRequests).isEmpty();
        }

        @Test
        void shouldNotScaleHostsForFargateLaunchType() {
            // Given
            instanceProperties.set(COMPACTION_ECS_LAUNCHTYPE, "FARGATE");

            // When
            runTasks(jobsOnQueue(3), noExistingTasks());

            // Then
            assertThat(scaleToHostsRequests).isEmpty();
            assertThat(launchTasksRequests).containsExactly(3);
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
        return autoScalingGroupMaxSizeByName::get;
    }

    private SetDesiredInstances setDesiredInstances() {
        return (group, desiredSize) -> scaleToHostsRequests.add(desiredSize);
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
        autoScalingGroupMaxSizeByName.put(instanceProperties.get(COMPACTION_AUTO_SCALING_GROUP), maxSize);
    }
}
