/*
 * Copyright 2022 Crown Copyright
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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static sleeper.util.ClientUtils.optionalArgument;

public class WaitForTasks {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForTasks.class);

    private final AmazonECS ecsClient;
    private final List<Task> tasks;

    public WaitForTasks(AmazonECS ecsClient, List<Task> tasks) {
        this.ecsClient = ecsClient;
        this.tasks = tasks;
    }

    private List<Task> describeTasks() {
        Map<String, List<Task>> tasksByCluster = tasks.stream()
                .collect(Collectors.groupingBy(Task::getClusterArn));
        return tasksByCluster.entrySet().stream()
                .flatMap(entry -> describeTasks(entry.getKey(), entry.getValue()).stream())
                .collect(Collectors.toList());
    }

    private List<Task> describeTasks(String cluster, List<Task> tasks) {
        DescribeTasksResult result = ecsClient.describeTasks(new DescribeTasksRequest().withCluster(cluster)
                .withTasks(tasks.stream().map(Task::getTaskArn).collect(Collectors.toList())));
        List<Failure> failures = result.getFailures();
        if (!failures.isEmpty()) {
            LOGGER.warn("Failures describing tasks for cluster {}: {}", cluster, failures);
        }
        return result.getTasks();
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: <instance id> <table name> <tasks file> <optional status format>");
            System.out.println("Status format can be status or full, defaults to status.");
            return;
        }

        List<Task> tasks = TasksJson.readTasksFromFile(Paths.get(args[2]));
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        String formatStr = optionalArgument(args, 3)
                .map(arg -> arg.toLowerCase(Locale.ROOT))
                .orElse("status");
        StatusFormat format = statusFormat(formatStr);
        WaitForTasks wait = new WaitForTasks(ecsClient, tasks);
        LOGGER.info("Task statuses: {}", format.statusOutput(wait.describeTasks()));
        ecsClient.shutdown();
    }

    public static StatusFormat statusFormat(String format) {
        switch (format) {
            case "full":
                return TasksJson::new;
            case "status":
            default:
                return tasks -> tasks.stream().map(TaskStatusJson::new).collect(Collectors.toList());
        }
    }

    interface StatusFormat {
        Object statusOutput(List<Task> tasks);
    }
}
