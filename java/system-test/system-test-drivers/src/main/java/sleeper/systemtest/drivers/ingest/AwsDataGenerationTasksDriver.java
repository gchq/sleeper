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

package sleeper.systemtest.drivers.ingest;

import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.RunTaskResponse;
import software.amazon.awssdk.services.ecs.model.Task;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.DataGenerationTasksDriver;

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.systemtest.drivers.ingest.WaitForGenerateData.ecsTaskStatusFormat;

public class AwsDataGenerationTasksDriver implements DataGenerationTasksDriver {
    private final DeployedSystemTestResources systemTest;
    private final SystemTestInstanceContext instance;
    private final EcsClient ecsClient;

    public AwsDataGenerationTasksDriver(
            DeployedSystemTestResources systemTest, SystemTestInstanceContext instance, EcsClient ecsClient) {
        this.systemTest = systemTest;
        this.instance = instance;
        this.ecsClient = ecsClient;
    }

    public void runDataGenerationTasks(PollWithRetries poll) {
        List<Task> tasks = startTasks();
        try {
            waitForTasks(tasks, poll);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private List<Task> startTasks() {
        List<RunTaskResponse> responses = new RunWriteRandomDataTaskOnECS(
                instance.getInstanceProperties(), instance.getTableProperties(), systemTest.getProperties(), ecsClient)
                .run();
        return responses.stream()
                .flatMap(response -> response.tasks().stream())
                .collect(Collectors.toUnmodifiableList());
    }

    private void waitForTasks(List<Task> tasks, PollWithRetries poll) throws InterruptedException {
        new WaitForGenerateData(ecsClient, tasks, ecsTaskStatusFormat("summary"))
                .pollUntilFinished(poll);
    }
}
