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
package sleeper.compaction.job.execution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.job.common.CommonJobUtils;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CompactionProperty.COMPACTION_ECS_LAUNCHTYPE;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

/**
 * Retrieves compaction {@link CompactionJob}s from an SQS queue, and executes
 * them. It delegates the actual execution of the job to an instance of
 * {@link CompactSortedFiles}, and the processing of SQS messages to {@link SqsCompactionQueueHandler}.
 */
public class CompactSortedFilesRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactSortedFilesRunner.class);

    private final InstanceProperties instanceProperties;
    private final ObjectFactory objectFactory;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final CompactionTaskStatusStore taskStatusStore;
    private final String taskId;
    private final AmazonSQS sqsClient;
    private final AmazonECS ecsClient;

    private CompactSortedFilesRunner(Builder builder) {
        instanceProperties = builder.instanceProperties;
        objectFactory = builder.objectFactory;
        tablePropertiesProvider = builder.tablePropertiesProvider;
        propertiesReloader = builder.propertiesReloader;
        stateStoreProvider = builder.stateStoreProvider;
        jobStatusStore = builder.jobStatusStore;
        taskStatusStore = builder.taskStatusStore;
        taskId = builder.taskId;
        sqsClient = builder.sqsClient;
        ecsClient = builder.ecsClient;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void run() throws InterruptedException, IOException {
        // Log some basic data if running on EC2 inside ECS
        if (instanceProperties.get(COMPACTION_ECS_LAUNCHTYPE).equalsIgnoreCase("EC2")) {
            try {
                if (this.ecsClient != null) {
                    CommonJobUtils.retrieveContainerMetadata(ecsClient).ifPresent(info -> LOGGER.info(
                            "Task running on EC2 instance ID {} in AZ {} with ARN {} in cluster {} with status {}",
                            info.instanceID, info.az, info.instanceARN, info.clusterName, info.status));
                } else {
                    LOGGER.warn("ECS client is null");
                }
            } catch (IOException e) {
                LOGGER.warn("EC2 instance data not available", e);
            }
        }

        SqsCompactionQueueHandler queueHandler = new SqsCompactionQueueHandler(sqsClient, instanceProperties);
        new CompactionTask(instanceProperties, Instant::now, queueHandler::receiveFromSqs,
                this::compact, taskStatusStore, taskId)
                .run();
    }

    private RecordsProcessedSummary compact(CompactionJob compactionJob) throws IteratorException, IOException, StateStoreException {
        propertiesReloader.reloadIfNeeded();
        CompactSortedFiles compactSortedFiles = new CompactSortedFiles(instanceProperties, tablePropertiesProvider, objectFactory,
                stateStoreProvider, jobStatusStore, taskId);
        return compactSortedFiles.run(compactionJob);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ObjectFactoryException {
        if (1 != args.length) {
            System.err.println("Error: must have 1 argument (config bucket), got " + args.length + " arguments (" + StringUtils.join(args, ',') + ")");
            System.exit(1);
        }

        AmazonDynamoDB dynamoDBClient = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        AmazonSQS sqsClient = buildAwsV1Client(AmazonSQSClientBuilder.standard());
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonECS ecsClient = buildAwsV1Client(AmazonECSClientBuilder.standard());

        String s3Bucket = args[0];
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, s3Bucket);

        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        PropertiesReloader propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties,
                HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient,
                instanceProperties);
        CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDBClient,
                instanceProperties);

        ObjectFactory objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");
        CompactSortedFilesRunner runner = CompactSortedFilesRunner.builder()
                .instanceProperties(instanceProperties)
                .objectFactory(objectFactory)
                .tablePropertiesProvider(tablePropertiesProvider)
                .propertiesReloader(propertiesReloader)
                .stateStoreProvider(stateStoreProvider)
                .jobStatusStore(jobStatusStore)
                .taskStatusStore(taskStatusStore)
                .taskId(UUID.randomUUID().toString())
                .sqsClient(sqsClient)
                .ecsClient(ecsClient)
                .build();
        runner.run();

        sqsClient.shutdown();
        LOGGER.info("Shut down sqsClient");
        dynamoDBClient.shutdown();
        LOGGER.info("Shut down dynamoDBClient");
        s3Client.shutdown();
        LOGGER.info("Shut down s3Client");
        ecsClient.shutdown();
        LOGGER.info("Shut down ecsClient");
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private ObjectFactory objectFactory;
        private TablePropertiesProvider tablePropertiesProvider;
        private PropertiesReloader propertiesReloader;
        private StateStoreProvider stateStoreProvider;
        private CompactionJobStatusStore jobStatusStore;
        private CompactionTaskStatusStore taskStatusStore;
        private String taskId;
        private AmazonSQS sqsClient;
        private AmazonECS ecsClient;

        private Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder tablePropertiesProvider(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
            return this;
        }

        public Builder propertiesReloader(PropertiesReloader propertiesReloader) {
            this.propertiesReloader = propertiesReloader;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder jobStatusStore(CompactionJobStatusStore jobStatusStore) {
            this.jobStatusStore = jobStatusStore;
            return this;
        }

        public Builder taskStatusStore(CompactionTaskStatusStore taskStatusStore) {
            this.taskStatusStore = taskStatusStore;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder sqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
            return this;
        }

        public Builder ecsClient(AmazonECS ecsClient) {
            this.ecsClient = ecsClient;
            return this;
        }

        public CompactSortedFilesRunner build() {
            return new CompactSortedFilesRunner(this);
        }
    }
}
