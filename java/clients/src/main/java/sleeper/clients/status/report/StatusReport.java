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
package sleeper.clients.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;

import sleeper.clients.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.partitions.PartitionsStatusReporter;
import sleeper.clients.util.ClientUtils;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import static sleeper.clients.util.ClientUtils.optionalArgument;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

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
    private final AmazonSQS sqsClient;
    private final TablePropertiesProvider tablePropertiesProvider;

    public StatusReport(InstanceProperties instanceProperties,
                        TableProperties tableProperties,
                        boolean verbose,
                        StateStore stateStore,
                        CompactionJobStatusStore compactionStatusStore,
                        CompactionTaskStatusStore compactionTaskStatusStore,
                        AmazonSQS sqsClient,
                        TablePropertiesProvider tablePropertiesProvider) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.verbose = verbose;
        this.stateStore = stateStore;
        this.compactionStatusStore = compactionStatusStore;
        this.compactionTaskStatusStore = compactionTaskStatusStore;
        this.sqsClient = sqsClient;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    private void run() throws StateStoreException {
        System.out.println("\nFull Status Report:\n--------------------------");
        // Partitions
        new PartitionsStatusReport(stateStore, tableProperties, new PartitionsStatusReporter(System.out)).run();

        // Data files
        new FilesStatusReport(stateStore, 1000, verbose).run();

        // Jobs
        new CompactionJobStatusReport(compactionStatusStore,
                new StandardCompactionJobStatusReporter(),
                tableProperties.get(TABLE_NAME),
                JobQuery.Type.UNFINISHED).run();

        // Tasks
        new CompactionTaskStatusReport(compactionTaskStatusStore,
                new StandardCompactionTaskStatusReporter(System.out),
                CompactionTaskQuery.UNFINISHED).run();

        // Dead letters
        new DeadLettersStatusReport(sqsClient, instanceProperties, tablePropertiesProvider).run();
    }

    public static void main(String[] args) throws StateStoreException {
        if (2 != args.length && 3 != args.length) {
            throw new IllegalArgumentException("Usage: <instance-id> <table-name> <optional-verbose-true-or-false>");
        }
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        InstanceProperties instanceProperties = ClientUtils.getInstanceProperties(amazonS3, args[0]);

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, amazonS3, dynamoDBClient);
        TableProperties tableProperties = tablePropertiesProvider.getByName(args[1]);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, new Configuration());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        CompactionJobStatusStore compactionStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
        CompactionTaskStatusStore compactionTaskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);

        boolean verbose = optionalArgument(args, 2)
                .map(Boolean::parseBoolean)
                .orElse(false);
        StatusReport statusReport = new StatusReport(
                instanceProperties, tableProperties, verbose,
                stateStore, compactionStatusStore, compactionTaskStatusStore,
                sqsClient, tablePropertiesProvider);
        amazonS3.shutdown();
        statusReport.run();
        sqsClient.shutdown();
        dynamoDBClient.shutdown();
    }
}
