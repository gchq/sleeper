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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Task;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.systemtest.drivers.ingest.WaitForGenerateData.ecsTaskStatusFormat;

public class DataGenerationDriver {
    private final SystemTestInstanceContext systemTest;
    private final SleeperInstanceContext instance;
    private final AmazonECS ecsClient;

    public DataGenerationDriver(SystemTestInstanceContext systemTest,
                                SleeperInstanceContext instance,
                                AmazonECS ecsClient) {
        this.systemTest = systemTest;
        this.instance = instance;
        this.ecsClient = ecsClient;
    }

    public List<Task> startTasks() {
        List<RunTaskResult> results = new RunWriteRandomDataTaskOnECS(
                instance.getInstanceProperties(), instance.getTableProperties(), systemTest.getProperties(), ecsClient)
                .run();
        return results.stream()
                .flatMap(result -> result.getTasks().stream())
                .collect(Collectors.toUnmodifiableList());
    }

    public void waitForTasks(List<Task> tasks, PollWithRetries poll) throws InterruptedException {
        new WaitForGenerateData(ecsClient, tasks, ecsTaskStatusFormat("summary"))
                .pollUntilFinished(poll);
    }
}
