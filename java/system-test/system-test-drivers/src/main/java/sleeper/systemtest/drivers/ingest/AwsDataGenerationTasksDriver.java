/*
 * Copyright 2022-2025 Crown Copyright
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
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.drivers.util.SystemTestClients;
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
    private final SqsClient sqsClient;

    public AwsDataGenerationTasksDriver(
            DeployedSystemTestResources systemTest, SystemTestInstanceContext instance, SystemTestClients clients) {
        this.systemTest = systemTest;
        this.instance = instance;
        this.ecsClient = clients.getEcs();
        this.sqsClient = clients.getSqsV2();
    }

    @Override
    public void runDataGenerationTasks(PollWithRetries poll) {
        List<Task> tasks = startTasks();
        waitForTasks(tasks, poll);
    }

    @Override
    public void runDataGenerationJobs(List<SystemTestDataGenerationJob> jobs, PollWithRetries poll) {
        jobSender().sendJobsToQueue(jobs);

        //Create ecs tasks to read the jobs from the queue and write the data.
    }

    private List<Task> startTasks() {
        List<RunTaskResponse> responses = taskStarter().run();
        return responses.stream()
                .flatMap(response -> response.tasks().stream())
                .collect(Collectors.toUnmodifiableList());
    }

    private void waitForTasks(List<Task> tasks, PollWithRetries poll) {
        try {
            new WaitForGenerateData(ecsClient, tasks, ecsTaskStatusFormat("summary"))
                    .pollUntilFinished(poll);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private SystemTestDataGenerationJobSender jobSender() {
        return new SystemTestDataGenerationJobSender(systemTest.getProperties(), sqsClient);
    }

    private RunWriteRandomDataTaskOnECS taskStarter() {
        return new RunWriteRandomDataTaskOnECS(instance.getInstanceProperties(), systemTest.getProperties(), ecsClient);
    }
}
