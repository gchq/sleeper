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
package sleeper.job.common;

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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class RunECSTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunECSTasks.class);
    private static final int CAPACITY_UNAVAILABLE_RETRY_INTERVAL_MILLIS = 30000;
    private static final int CAPACITY_UNAVAILABLE_RETRY_MAX_POLLS = 10;

    private final AmazonECS ecsClient;
    private final RunTaskRequest runTaskRequest;
    private final int numberOfTasksToCreate;
    private final BooleanSupplier checkAbort;
    private final Consumer<RunTaskResult> resultConsumer;
    private final PollWithRetries retryWhenNoCapacity;

    private RunECSTasks(Builder builder) {
        ecsClient = builder.ecsClient;
        runTaskRequest = builder.runTaskRequest;
        numberOfTasksToCreate = builder.numberOfTasksToCreate;
        checkAbort = builder.checkAbort;
        resultConsumer = builder.resultConsumer;
        retryWhenNoCapacity = builder.retryWhenNoCapacity;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void runTasks(AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate) {
        builder().ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .build().runTasks();
    }

    public static void runTasks(AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        builder().ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .checkAbort(checkAbort)
                .build().runTasks();
    }

    public static void runTasksOrThrow(
            AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate)
            throws AmazonClientException {
        builder().ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .build().runTasksOrThrow();
    }

    public static void runTasksOrThrow(
            AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate, BooleanSupplier checkAbort)
            throws AmazonClientException {
        builder().ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .checkAbort(checkAbort)
                .build().runTasksOrThrow();
    }

    public static void runTasksOrThrow(
            AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate, Consumer<RunTaskResult> resultConsumer)
            throws AmazonClientException {
        builder().ecsClient(ecsClient)
                .runTaskRequest(runTaskRequest)
                .numberOfTasksToCreate(numberOfTasksToCreate)
                .resultConsumer(resultConsumer)
                .build().runTasksOrThrow();
    }

    public void runTasks() {
        try {
            runTasksOrThrow();
        } catch (InvalidParameterException e) {
            LOGGER.error("Couldn't launch tasks due to InvalidParameterException. " +
                    "This error is expected if there are no EC2 container instances in the cluster.");
        } catch (AmazonClientException e) {
            LOGGER.error("Couldn't launch tasks", e);
        }
    }

    public void runTasksOrThrow()
            throws AmazonClientException {
        LOGGER.info("Creating {} tasks", numberOfTasksToCreate);
        for (int i = 0; i < numberOfTasksToCreate; i += 10) {
            if (i > 0) {
                // Rate limit for Fargate tasks is 100 burst, 20 sustained.
                // Rate limit for ECS task creation API is 20 burst, 20 sustained.
                // To stay below this limit we create 10 tasks once per second.
                // See documentation:
                // https://docs.aws.amazon.com/AmazonECS/latest/userguide/throttling.html
                sleepForSustainedRatePerSecond(1);
            }
            int remainingTasksToCreate = numberOfTasksToCreate - i;
            int tasksToCreateThisRound = Math.min(10, remainingTasksToCreate);

            RunTaskResult runTaskResult = ecsClient.runTask(runTaskRequest.withCount(tasksToCreateThisRound));
            LOGGER.info("Submitted RunTaskRequest (cluster = {}, type = {}, container name = {}, task definition = {})",
                    runTaskRequest.getCluster(), runTaskRequest.getLaunchType(),
                    new ContainerName(runTaskResult), new TaskDefinitionArn(runTaskResult));
            if (runTaskResult.getFailures().stream().anyMatch(RunECSTasks::isCapacityUnavailable)) {
                LOGGER.info("No capacity was available, retrying request until there is");
                try {
                    runTaskResult = retryTaskUntilCapacityAvailable(ecsClient, runTaskRequest);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Failed to retry task ", e);
                }
            }
            if (checkFailure(runTaskResult)) {
                throw new ECSFailureException("Failures running task " + i + ": " + runTaskResult.getFailures());
            }
            resultConsumer.accept(runTaskResult);

            if (checkAbort.getAsBoolean()) {
                LOGGER.info("Aborting running ECS tasks");
                return;
            }
        }
    }

    private RunTaskResult retryTaskUntilCapacityAvailable(AmazonECS ecsClient, RunTaskRequest request) throws InterruptedException {
        AtomicReference<RunTaskResult> atomicResult = new AtomicReference<>();
        retryWhenNoCapacity.pollUntil("capacity was available", () -> {
            RunTaskResult result = ecsClient.runTask(request);
            LOGGER.info("Retried task with failures: {}", result.getFailures());
            atomicResult.set(result);
            return result.getFailures().stream().noneMatch(RunECSTasks::isCapacityUnavailable);
        });
        LOGGER.info("Capacity is available");
        return atomicResult.get();
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

    /**
     * Checks for failures in run task results.
     *
     * @param runTaskResult result from recent run_tasks API call.
     * @return true if a task launch failure occurs
     */
    private static boolean checkFailure(RunTaskResult runTaskResult) {
        if (!runTaskResult.getFailures().isEmpty()) {
            LOGGER.warn("Run task request has {} failures", runTaskResult.getFailures().size());
            for (Failure f : runTaskResult.getFailures()) {
                LOGGER.error("Failure: ARN {} Reason {} Detail {}", f.getArn(), f.getReason(),
                        f.getDetail());
            }
            return true;
        }
        return false;
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
        private PollWithRetries retryWhenNoCapacity = PollWithRetries.intervalAndMaxPolls(
                CAPACITY_UNAVAILABLE_RETRY_INTERVAL_MILLIS, CAPACITY_UNAVAILABLE_RETRY_MAX_POLLS);

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

        public Builder retryWhenNoCapacity(PollWithRetries retryWhenNoCapacity) {
            this.retryWhenNoCapacity = retryWhenNoCapacity;
            return this;
        }

        public RunECSTasks build() {
            return new RunECSTasks(this);
        }
    }
}
