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
package sleeper.compaction.job.creation.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.creation.CreateCompactionJobs;
import sleeper.compaction.job.creation.CreateCompactionJobs.Mode;
import sleeper.compaction.job.creation.SendCompactionJobToSqs;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.jars.ObjectFactoryException;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.util.LoggedDuration;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda function for executing {@link CreateCompactionJobs}.
 */
@SuppressWarnings("unused")
public class CreateCompactionJobsLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateCompactionJobsLambda.class);

    private final AmazonDynamoDB dynamoDBClient;
    private final AmazonSQS sqsClient;
    private final InstanceProperties instanceProperties;
    private final ObjectFactory objectFactory;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final PropertiesReloader propertiesReloader;
    private final StateStoreProvider stateStoreProvider;
    private final CompactionJobStatusStore jobStatusStore;
    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();

    /**
     * No-args constructor used by Lambda service. Dynamo file table name will be obtained from an environment variable.
     *
     * @throws ObjectFactoryException if user jars cannot be loaded
     */
    public CreateCompactionJobsLambda() throws ObjectFactoryException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        String s3Bucket = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());

        this.instanceProperties = new InstanceProperties();
        this.instanceProperties.loadFromS3(s3Client, s3Bucket);

        this.objectFactory = new ObjectFactory(instanceProperties, s3Client, "/tmp");

        this.dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        this.tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoDBClient);
        this.propertiesReloader = PropertiesReloader.ifConfigured(s3Client, instanceProperties, tablePropertiesProvider);
        Configuration conf = HadoopConfigurationProvider.getConfigurationForLambdas(instanceProperties);
        this.stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties, conf);
        this.jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDBClient, instanceProperties);
    }

    public Void handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);
        propertiesReloader.reloadIfNeeded();

        CreateCompactionJobs createJobs = new CreateCompactionJobs(
                objectFactory, instanceProperties, tablePropertiesProvider, stateStoreProvider,
                new SendCompactionJobToSqs(instanceProperties, sqsClient)::send, jobStatusStore, Mode.STRATEGY);

        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message: {}", body))
                .map(serDe::fromJson)
                .forEach(createJobs::createJobs);

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }
}
