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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.ecs.model.DescribeTasksRequest;
import software.amazon.awssdk.services.ecs.model.DescribeTasksResponse;
import software.amazon.awssdk.services.ecs.model.Failure;
import software.amazon.awssdk.services.ecs.model.Task;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.ingest.json.TaskStatusJson;
import sleeper.systemtest.drivers.ingest.json.TasksJson;
import sleeper.systemtest.drivers.ingest.json.TasksSummaryJson;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.min;
import static sleeper.clients.util.ClientUtils.optionalArgument;

public class WaitForGenerateData {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForGenerateData.class);
    private static final Set<String> FINISHED_STATUSES = Stream.of("STOPPED", "DELETED").collect(Collectors.toSet());
    private static final PollWithRetries DEFAULT_POLL = PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(30), Duration.ofMinutes(15));

    private final EcsClient ecsClient;
    private final List<Task> generateDataTasks;
    private final ECSTaskStatusFormat ecsStatusFormat;

    public WaitForGenerateData(
            EcsClient ecsClient,
            List<Task> generateDataTasks,
            ECSTaskStatusFormat ecsStatusFormat) {
        this.ecsClient = ecsClient;
        this.generateDataTasks = generateDataTasks;
        this.ecsStatusFormat = ecsStatusFormat;
    }

    public void pollUntilFinished() throws InterruptedException {
        pollUntilFinished(DEFAULT_POLL);
    }

    public void pollUntilFinished(PollWithRetries poll) throws InterruptedException {
        poll.pollUntil("generate data tasks finished", this::isGenerateDataTasksFinished);
    }

    private boolean isGenerateDataTasksFinished() {
        List<Task> tasks = describeGenerateDataTasks();
        LOGGER.info("Generate data task statuses: {}", ecsStatusFormat.statusOutput(tasks));
        return tasks.stream().allMatch(task -> FINISHED_STATUSES.contains(task.lastStatus()));
    }

    private List<Task> describeGenerateDataTasks() {
        Map<String, List<Task>> tasksByCluster = generateDataTasks.stream()
                .collect(Collectors.groupingBy(Task::clusterArn));
        return tasksByCluster.entrySet().stream()
                .flatMap(entry -> describeTasks(entry.getKey(), entry.getValue()).stream())
                .collect(Collectors.toList());
    }

    private List<Task> describeTasks(String cluster, List<Task> tasks) {
        List<DescribeTasksRequest> tasksRequests = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i += 100) {
            tasksRequests.add(DescribeTasksRequest.builder().cluster(cluster).tasks(
                    tasks.subList(i, min(i + 100, tasks.size()))
                            .stream().map(Task::taskArn).collect(Collectors.toList()))
                    .build());
        }
        return tasksRequests.stream()
                .map(ecsClient::describeTasks)
                .peek(result -> {
                    List<Failure> failures = result.failures();
                    if (!failures.isEmpty()) {
                        LOGGER.warn("Failures describing tasks for cluster {}: {}", cluster, failures);
                    }
                })
                .map(DescribeTasksResponse::tasks).flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 2) {
            System.out.println("Usage: <generate data tasks file> <optional status format>");
            System.out.println("Status format can be status or full, defaults to status.");
            return;
        }

        List<Task> generateDataTasks = TasksJson.readTasksFromFile(Paths.get(args[0]));
        String statusFormatStr = optionalArgument(args, 1)
                .map(arg -> arg.toLowerCase(Locale.ROOT))
                .orElse("summary");
        ECSTaskStatusFormat ecsTaskFormat = ecsTaskStatusFormat(statusFormatStr);

        try (EcsClient ecsClient = EcsClient.create()) {
            WaitForGenerateData wait = new WaitForGenerateData(ecsClient, generateDataTasks, ecsTaskFormat);
            wait.pollUntilFinished();
        }
    }

    public static ECSTaskStatusFormat ecsTaskStatusFormat(String format) {
        switch (format) {
            case "full":
                return TasksJson::new;
            case "status":
                return tasks -> tasks.stream().map(TaskStatusJson::new).collect(Collectors.toList());
            case "summary":
            default:
                return TasksSummaryJson::new;
        }
    }

    public interface ECSTaskStatusFormat {
        Object statusOutput(List<Task> tasks);
    }
}
