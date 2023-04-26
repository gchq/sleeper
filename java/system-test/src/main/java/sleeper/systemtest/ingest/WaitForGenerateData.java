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
package sleeper.systemtest.ingest;

import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.util.PollWithRetries;
import sleeper.systemtest.ingest.json.TaskStatusJson;
import sleeper.systemtest.ingest.json.TasksJson;

import java.io.IOException;
import java.nio.file.Paths;
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
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final int MAX_POLLS = 30;

    private final AmazonECS ecsClient;
    private final List<Task> generateDataTasks;
    private final ECSTaskStatusFormat ecsStatusFormat;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public WaitForGenerateData(
            AmazonECS ecsClient,
            List<Task> generateDataTasks,
            ECSTaskStatusFormat ecsStatusFormat) {
        this.ecsClient = ecsClient;
        this.generateDataTasks = generateDataTasks;
        this.ecsStatusFormat = ecsStatusFormat;
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("generate data tasks finished", this::isGenerateDataTasksFinished);
    }

    private boolean isGenerateDataTasksFinished() {
        List<Task> tasks = describeGenerateDataTasks();
        LOGGER.info("Generate data task statuses: {}", ecsStatusFormat.statusOutput(tasks));
        return tasks.stream().allMatch(task -> FINISHED_STATUSES.contains(task.getLastStatus()));
    }

    private List<Task> describeGenerateDataTasks() {
        Map<String, List<Task>> tasksByCluster = generateDataTasks.stream()
                .collect(Collectors.groupingBy(Task::getClusterArn));
        return tasksByCluster.entrySet().stream()
                .flatMap(entry -> describeTasks(entry.getKey(), entry.getValue()).stream())
                .collect(Collectors.toList());
    }

    private List<Task> describeTasks(String cluster, List<Task> tasks) {
        List<DescribeTasksRequest> tasksRequests = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i += 100) {
            tasksRequests.add(new DescribeTasksRequest().withCluster(cluster).withTasks(
                    tasks.subList(i, min(i + 100, tasks.size()))
                            .stream().map(Task::getTaskArn).collect(Collectors.toList())));
        }
        return tasksRequests.stream()
                .map(ecsClient::describeTasks)
                .peek(result -> {
                    List<Failure> failures = result.getFailures();
                    if (!failures.isEmpty()) {
                        LOGGER.warn("Failures describing tasks for cluster {}: {}", cluster, failures);
                    }
                })
                .map(DescribeTasksResult::getTasks).flatMap(List::stream)
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
                .orElse("status");
        ECSTaskStatusFormat ecsTaskFormat = ecsTaskStatusFormat(statusFormatStr);

        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();

        WaitForGenerateData wait = new WaitForGenerateData(ecsClient, generateDataTasks, ecsTaskFormat);
        wait.pollUntilFinished();
        ecsClient.shutdown();
    }

    public static ECSTaskStatusFormat ecsTaskStatusFormat(String format) {
        switch (format) {
            case "full":
                return TasksJson::new;
            case "status":
            default:
                return tasks -> tasks.stream().map(TaskStatusJson::new).collect(Collectors.toList());
        }
    }

    interface ECSTaskStatusFormat {
        Object statusOutput(List<Task> tasks);
    }
}
