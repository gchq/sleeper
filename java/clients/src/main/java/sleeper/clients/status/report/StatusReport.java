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
package sleeper.clients.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.clients.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.partitions.PartitionsStatusReporter;
import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.StateStoreFactory;
import sleeper.task.common.QueueMessageCount;

import static sleeper.clients.util.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * A utility class to report information about the partitions, the files, the
 * jobs, and the compaction tasks in the system.
 */
public class StatusReport {
    private final InstanceProperties instanceProperties;
    private final TableProperties tableProperties;
    private final boolean verbose;
    private final StateStore stateStore;
    private final CompactionJobStatusStore compactionStatusStore;
    private final CompactionTaskStatusStore compactionTaskStatusStore;
    private final SqsClient sqsClient;
    private final QueueMessageCount.Client messageCount;
    private final TablePropertiesProvider tablePropertiesProvider;

    public StatusReport(
            InstanceProperties instanceProperties, TableProperties tableProperties,
            boolean verbose, StateStore stateStore,
            CompactionJobStatusStore compactionStatusStore, CompactionTaskStatusStore compactionTaskStatusStore,
            SqsClient sqsClient, QueueMessageCount.Client messageCount, TablePropertiesProvider tablePropertiesProvider) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.verbose = verbose;
        this.stateStore = stateStore;
        this.compactionStatusStore = compactionStatusStore;
        this.compactionTaskStatusStore = compactionTaskStatusStore;
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
        new CompactionJobStatusReport(compactionStatusStore,
                new StandardCompactionJobStatusReporter(),
                tableProperties.getStatus(),
                JobQuery.Type.UNFINISHED).run();

        // Tasks
        new CompactionTaskStatusReport(compactionTaskStatusStore,
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

        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClientV1 = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        try (SqsClient sqsClient = buildAwsV2Client(SqsClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = S3TableProperties.createProvider(instanceProperties, s3Client, dynamoDBClient);
            TableProperties tableProperties = tablePropertiesProvider.getByName(tableName);
            StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3Client, dynamoDBClient, new Configuration());
            StateStore stateStore = stateStoreFactory.getStateStore(tableProperties);
            CompactionJobStatusStore compactionStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
            CompactionTaskStatusStore compactionTaskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

            StatusReport statusReport = new StatusReport(
                    instanceProperties, tableProperties, verbose,
                    stateStore, compactionStatusStore, compactionTaskStatusStore,
                    sqsClient, QueueMessageCount.withSqsClient(sqsClientV1), tablePropertiesProvider);
            statusReport.run();
        } finally {
            s3Client.shutdown();
            dynamoDBClient.shutdown();
            sqsClientV1.shutdown();
        }
    }
}
