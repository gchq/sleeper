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
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.util.ClientUtils.optionalArgument;

public class WaitForIngest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForIngest.class);
    private static final Set<String> FINISHED_STATUSES = Stream.of("STOPPED", "DELETED").collect(Collectors.toSet());
    private static final long POLL_INTERVAL_MILLIS = 30000;
    private static final long MAX_POLLS = 30;

    private final AmazonECS ecsClient;
    private final List<Task> generateDataTasks;
    private final StatusFormat statusFormat;
    private long polls;

    public WaitForIngest(AmazonECS ecsClient, List<Task> generateDataTasks, StatusFormat statusFormat) {
        this.ecsClient = ecsClient;
        this.generateDataTasks = generateDataTasks;
        this.statusFormat = statusFormat;
    }

    public void pollUntilFinished() throws InterruptedException {
        pollUntil(this::isGenerateDataTasksFinished);
    }

    private void pollUntil(BooleanSupplier checkFinished) throws InterruptedException {
        while (polls < MAX_POLLS && !checkFinished.getAsBoolean()) {
            Thread.sleep(POLL_INTERVAL_MILLIS);
            polls++;
        }
    }

    private boolean isGenerateDataTasksFinished() {
        List<Task> tasks = describeGenerateDataTasks();
        LOGGER.info("Task statuses: {}", statusFormat.statusOutput(tasks));
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
        DescribeTasksResult result = ecsClient.describeTasks(new DescribeTasksRequest().withCluster(cluster)
                .withTasks(tasks.stream().map(Task::getTaskArn).collect(Collectors.toList())));
        List<Failure> failures = result.getFailures();
        if (!failures.isEmpty()) {
            LOGGER.warn("Failures describing tasks for cluster {}: {}", cluster, failures);
        }
        return result.getTasks();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 3 || args.length > 4) {
            System.out.println("Usage: <instance id> <table name> <generate data tasks file> <optional status format>");
            System.out.println("Status format can be status or full, defaults to status.");
            return;
        }

        List<Task> generateDataTasks = TasksJson.readTasksFromFile(Paths.get(args[2]));
        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        String formatStr = optionalArgument(args, 3)
                .map(arg -> arg.toLowerCase(Locale.ROOT))
                .orElse("status");
        StatusFormat format = statusFormat(formatStr);
        WaitForIngest wait = new WaitForIngest(ecsClient, generateDataTasks, format);
        wait.pollUntilFinished();
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
