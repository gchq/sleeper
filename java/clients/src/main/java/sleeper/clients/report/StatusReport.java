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
package sleeper.clients.report;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.clients.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.partitions.PartitionsStatusReporter;
import sleeper.common.task.QueueMessageCount;
import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.statestore.StateStoreFactory;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;

/**
 * Creates reports on the partitions, files and compactions in a Sleeper table, as well as tasks that run those
 * compaction jobs, and dead letters in the Sleeper instance.
 */
public class StatusReport {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final boolean verbose;
    private final StateStore stateStore;
    private final CompactionJobTracker compactionJobTracker;
    private final CompactionTaskTracker compactionTaskTracker;
    private final SqsClient sqsClient;
    private final QueueMessageCount.Client messageCount;
    private final TablePropertiesProvider tablePropertiesProvider;

    public StatusReport(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            boolean verbose, StateStore stateStore,
            CompactionJobTracker compactionJobTracker, CompactionTaskTracker compactionTaskTracker,
            SqsClient sqsClient, QueueMessageCount.Client messageCount, TablePropertiesProvider tablePropertiesProvider) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.verbose = verbose;
        this.stateStore = stateStore;
        this.compactionJobTracker = compactionJobTracker;
        this.compactionTaskTracker = compactionTaskTracker;
        this.sqsClient = sqsClient;
        this.messageCount = messageCount;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    private void run() {
        System.out.println("\nFull Status Report:\n--------------------------");
        // Partitions
        new PartitionsStatusReport(stateStore, tableProperties, new PartitionsStatusReporter(System.out)).run();

        // Data files
        new FilesStatusReport(stateStore, 1000, verbose).run();

        // Jobs
        new CompactionJobStatusReport(compactionJobTracker,
                new StandardCompactionJobStatusReporter(),
                tableProperties.getStatus(),
                JobQuery.Type.UNFINISHED).run();

        // Tasks
        new CompactionTaskStatusReport(compactionTaskTracker,
                new StandardCompactionTaskStatusReporter(System.out),
                CompactionTaskQuery.UNFINISHED).run();

        // Dead letters
        new DeadLettersStatusReport(sqsClient, messageCount, instanceProperties, tablePropertiesProvider).run();
    }

    public static void main(String[] args) {
        if (2 != args.length && 3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <optional-verbose-true-or-false>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        boolean verbose = optionalArgument(args, 2)
                .map(Boolean::parseBoolean)
                .orElse(false);

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoClient);
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoClient);
            StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
            CompactionJobTracker compactionJobTracker = CompactionJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
            CompactionTaskTracker compactionTaskTracker = CompactionTaskTrackerFactory.getTracker(dynamoClient, instanceProperties);

            StatusReport statusReport = new StatusReport(
                    instanceProperties, tableProperties, verbose,
                    stateStore, compactionJobTracker, compactionTaskTracker,
                    sqsClient, QueueMessageCount.withSqsClient(sqsClient), tablePropertiesProvider);
            statusReport.run();
        }
    }
}
