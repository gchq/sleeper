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

import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static sleeper.core.util.RateLimitUtils.sleepForSustainedRatePerSecond;

public class RunECSTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunECSTasks.class);

    private RunECSTasks() {
    }

    public static void runTasks(AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate) {
        runTasks(ecsClient, runTaskRequest, numberOfTasksToCreate, () -> false);
    }

    public static void runTasks(AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate, BooleanSupplier checkAbort) {
        try {
            runTasksOrThrow(ecsClient, runTaskRequest, numberOfTasksToCreate, checkAbort);
        } catch (InvalidParameterException e) {
            LOGGER.error("Couldn't launch tasks due to InvalidParameterException. " +
                    "This error is expected if there are no EC2 container instances in the cluster.");
        } catch (AmazonClientException e) {
            LOGGER.error("Couldn't launch tasks", e);
        }
    }

    public static void runTasksOrThrow(
            AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate)
            throws AmazonClientException {
        runTasksOrThrow(ecsClient, runTaskRequest, numberOfTasksToCreate, () -> false);
    }

    public static void runTasksOrThrow(
            AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate, BooleanSupplier checkAbort)
            throws AmazonClientException {
        runTasksOrThrow(ecsClient, runTaskRequest, numberOfTasksToCreate, checkAbort, result -> {
        });
    }

    public static void runTasksOrThrow(
            AmazonECS ecsClient, RunTaskRequest runTaskRequest, int numberOfTasksToCreate, BooleanSupplier checkAbort, Consumer<RunTaskResult> resultConsumer)
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
            resultConsumer.accept(runTaskResult);
            if (checkFailure(runTaskResult)) {
                throw new ECSFailureException("Failures running task " + i + ": " + runTaskResult.getFailures());
            }

            if (checkAbort.getAsBoolean()) {
                LOGGER.info("Aborting running ECS tasks");
                return;
            }
        }
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
}
