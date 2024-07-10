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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.Container;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.InvalidParameterException;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.PollWithRetries;
import sleeper.core.util.RateLimitUtils;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;

public class RunECSTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunECSTasks.class);
    private static final PollWithRetries DEFAULT_CAPACITY_UNAVAILABLE_RETRY = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(1));

    private final AmazonECS ecsClient;
    private final RunTaskRequest runTaskRequest;
    private final int numberOfTasksToCreate;
    private final BooleanSupplier checkAbort;
    private final Consumer<RunTaskResult> resultConsumer;
    private final DoubleConsumer sleepForSustainedRatePerSecond;
    private final PollWithRetries retryWhenNoCapacity;

    private RunECSTasks(Builder builder) {
        ecsClient = builder.ecsClient;
        runTaskRequest = builder.runTaskRequest;
        numberOfTasksToCreate = builder.numberOfTasksToCreate;
        checkAbort = builder.checkAbort;
        resultConsumer = builder.resultConsumer;
        sleepForSustainedRatePerSecond = builder.sleepForSustainedRatePerSecond;
        retryWhenNoCapacity = builder.retryWhenNoCapacity;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void runTasks(AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate) {
        runTasks(builder -> builder.ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate));
    }

    public static void runTasks(Consumer<RunECSTasks.Builder> configuration) {
        Builder builder = builder();
        configuration.accept(builder);
        builder.build().runTasks();
    }

    public static void runTasksOrThrow(Consumer<RunECSTasks.Builder> configuration) {
        Builder builder = builder();
        configuration.accept(builder);
        builder.build().runTasksOrThrow();
    }

    public void runTasks() {
        try {
            runTasksOrThrow();
        } catch (InvalidParameterException e) {
            LOGGER.error("Couldn't launch tasks due to InvalidParameterException. " +
                    "This error is expected if there are no EC2 container instances in the cluster.", e);
        } catch (AmazonClientException | PollWithRetries.CheckFailedException | ECSFailureException e) {
            LOGGER.error("Couldn't launch tasks", e);
        }
    }

    public void runTasksOrThrow() throws AmazonClientException {
        LOGGER.info("Creating {} tasks", numberOfTasksToCreate);
        try {
            int remainingTasks = numberOfTasksToCreate;
            while (true) {

                try {
                    remainingTasks = retryTaskUntilCapacityAvailable(remainingTasks);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed to retry task ", e);
                }
                if (remainingTasks > 0) {
                    // Rate limit for Fargate tasks is 100 burst, 20 sustained.
                    // Rate limit for ECS task creation API is 20 burst, 20 sustained.
                    // To stay below this limit we create 10 tasks once per second.
                    // See documentation:
                    // https://docs.aws.amazon.com/AmazonECS/latest/userguide/throttling.html
                    sleepForSustainedRatePerSecond.accept(1);
                } else {
                    break;
                }
            }
        } catch (ECSAbortException e) {
            LOGGER.info("Aborted running ECS tasks");
        }
    }

    private int retryTaskUntilCapacityAvailable(int remainingTasks) throws InterruptedException {
        AtomicInteger remainingTasksObj = new AtomicInteger(remainingTasks);
        retryWhenNoCapacity.pollUntil("capacity was available", () -> {
            int tasksThisRound = Math.min(10, remainingTasksObj.get());
            RunTaskResult result = ecsClient.runTask(runTaskRequest.withCount(tasksThisRound));
            resultConsumer.accept(result);
            if (checkAbort.getAsBoolean()) {
                throw new ECSAbortException();
            }

            int failures = result.getFailures().size();
            int capacityUnavailableFailures = (int) result.getFailures().stream()
                    .filter(RunECSTasks::isCapacityUnavailable).count();
            int fatalFailures = failures - capacityUnavailableFailures;
            int successfulTaskRuns = tasksThisRound - failures;
            int remainingTasksAfter = remainingTasksObj.updateAndGet(tasks -> tasks - successfulTaskRuns);

            LOGGER.info("Submitted RunTaskRequest (cluster = {}, type = {}, container name = {}, task definition = {})",
                    runTaskRequest.getCluster(), runTaskRequest.getLaunchType(),
                    new ContainerName(result), new TaskDefinitionArn(result));
            LOGGER.info("Found failures: {}", result.getFailures());
            LOGGER.info("Created {} tasks, {} remaining to create", result.getTasks().size(), remainingTasksAfter);
            if (fatalFailures > 0) {
                throw new ECSFailureException("Failures running task: " + result.getFailures());
            }

            return capacityUnavailableFailures == 0;
        });
        return remainingTasksObj.get();
    }

    private static class ContainerName {
        private final RunTaskResult runTaskResult;

        private ContainerName(RunTaskResult runTaskResult) {
            this.runTaskResult = runTaskResult;
        }

        public String toString() {
            return runTaskResult.getTasks().stream()
                    .flatMap(task -> task.getContainers().stream())
                    .map(Container::getName)
                    .findFirst().orElse("none");
        }
    }

    private static class TaskDefinitionArn {
        private final RunTaskResult runTaskResult;

        private TaskDefinitionArn(RunTaskResult runTaskResult) {
            this.runTaskResult = runTaskResult;
        }

        public String toString() {
            return runTaskResult.getTasks().stream()
                    .map(Task::getTaskDefinitionArn)
                    .findFirst().orElse("none");
        }
    }

    private static boolean isCapacityUnavailable(Failure failure) {
        return failure.getReason().equals("Capacity is unavailable at this time. Please try again later or in a different availability zone");
    }

    public static final class Builder {
        private AmazonECS ecsClient;
        private RunTaskRequest runTaskRequest;
        private int numberOfTasksToCreate;
        private BooleanSupplier checkAbort = () -> false;
        private Consumer<RunTaskResult> resultConsumer = result -> {
        };
        private DoubleConsumer sleepForSustainedRatePerSecond = RateLimitUtils::sleepForSustainedRatePerSecond;
        private PollWithRetries retryWhenNoCapacity = DEFAULT_CAPACITY_UNAVAILABLE_RETRY;

        private Builder() {
        }

        public Builder ecsClient(AmazonECS ecsClient) {
            this.ecsClient = ecsClient;
            return this;
        }

        public Builder runTaskRequest(RunTaskRequest runTaskRequest) {
            this.runTaskRequest = runTaskRequest;
            return this;
        }

        public Builder numberOfTasksToCreate(int numberOfTasksToCreate) {
            this.numberOfTasksToCreate = numberOfTasksToCreate;
            return this;
        }

        public Builder checkAbort(BooleanSupplier checkAbort) {
            this.checkAbort = checkAbort;
            return this;
        }

        public Builder resultConsumer(Consumer<RunTaskResult> resultConsumer) {
            this.resultConsumer = resultConsumer;
            return this;
        }

        public Builder sleepForSustainedRatePerSecond(DoubleConsumer sleepForSustainedRatePerSecond) {
            this.sleepForSustainedRatePerSecond = sleepForSustainedRatePerSecond;
            return this;
        }

        public Builder retryWhenNoCapacity(PollWithRetries retryWhenNoCapacity) {
            this.retryWhenNoCapacity = retryWhenNoCapacity;
            return this;
        }

        public RunECSTasks build() {
            return new RunECSTasks(this);
        }
    }
}
