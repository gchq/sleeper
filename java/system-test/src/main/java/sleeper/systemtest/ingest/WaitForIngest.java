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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.ingest.status.store.task.DynamoDBIngestTaskStatusStore;
import sleeper.ingest.task.IngestTaskStatusStore;
import sleeper.systemtest.SystemTestProperties;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
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
    private final IngestTaskStatusStore ingestTaskStatusStore;
    private long polls;

    public WaitForIngest(
            AmazonECS ecsClient,
            List<Task> generateDataTasks,
            StatusFormat statusFormat,
            IngestTaskStatusStore ingestTaskStatusStore) {
        this.ecsClient = ecsClient;
        this.generateDataTasks = generateDataTasks;
        this.statusFormat = statusFormat;
        this.ingestTaskStatusStore = ingestTaskStatusStore;
    }

    public void pollUntilFinished() throws InterruptedException {
        pollUntil("generate data tasks finished", this::isGenerateDataTasksFinished);
        Instant generateDataStartTime = getGenerateDataStartTime();
        pollUntil("ingest tasks started", () -> !ingestTaskStatusStore.getTasksInTimePeriod(generateDataStartTime, Instant.now()).isEmpty());
        pollUntil("ingest tasks finished", () -> ingestTaskStatusStore.getTasksInProgress().isEmpty());
    }

    private void pollUntil(String description, BooleanSupplier checkFinished) throws InterruptedException {
        while (!checkFinished.getAsBoolean()) {
            if (polls >= MAX_POLLS) {
                throw new TimedOutException("Timed out waiting until " + description);
            }
            Thread.sleep(POLL_INTERVAL_MILLIS);
            polls++;
        }
    }

    private Instant getGenerateDataStartTime() {
        return generateDataTasks.stream()
                .map(task -> task.getStartedAt().toInstant())
                .sorted().findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Found no generate data task start time"));
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
        if (args.length < 2 || args.length > 3) {
            System.out.println("Usage: <instance id> <generate data tasks file> <optional status format>");
            System.out.println("Status format can be status or full, defaults to status.");
            return;
        }

        String instanceId = args[0];
        List<Task> generateDataTasks = TasksJson.readTasksFromFile(Paths.get(args[1]));
        String statusFormatStr = optionalArgument(args, 2)
                .map(arg -> arg.toLowerCase(Locale.ROOT))
                .orElse("status");
        StatusFormat statusFormat = statusFormat(statusFormatStr);

        AmazonECS ecsClient = AmazonECSClientBuilder.defaultClient();
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        SystemTestProperties systemTestProperties = new SystemTestProperties();
        systemTestProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        IngestTaskStatusStore ingestTaskStatusStore = DynamoDBIngestTaskStatusStore.from(dynamoDBClient, systemTestProperties);

        WaitForIngest wait = new WaitForIngest(ecsClient, generateDataTasks, statusFormat, ingestTaskStatusStore);
        wait.pollUntilFinished();
        ecsClient.shutdown();
        s3Client.shutdown();
        dynamoDBClient.shutdown();
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

    private static class TimedOutException extends RuntimeException {
        private TimedOutException(String message) {
            super(message);
        }
    }
}
