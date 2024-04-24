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
package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.table.InvokeForTableRequest;
import sleeper.core.table.InvokeForTableRequestSerDe;
import sleeper.core.util.LoggedDuration;
import sleeper.statestore.transactionlog.TransactionLogSnapshotCreator;

import java.time.Instant;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * A lambda that receives batches of tables from an SQS queue and creates transaction log snapshots for them.
 */
public class TransactionLogSnapshotCreationLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreationLambda.class);

    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final String configBucketName;
    private final InvokeForTableRequestSerDe serDe = new InvokeForTableRequestSerDe();

    public TransactionLogSnapshotCreationLambda() {
        this(
                AmazonS3ClientBuilder.defaultClient(),
                AmazonDynamoDBClientBuilder.defaultClient(),
                System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
    }

    public TransactionLogSnapshotCreationLambda(AmazonS3 s3Client, AmazonDynamoDB dynamoClient, String configBucketName) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
        this.configBucketName = configBucketName;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda started at {}", startTime);

        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .peek(body -> LOGGER.info("Received message: {}", body))
                .map(serDe::fromJson)
                .forEach(this::createSnapshots);

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

    private void createSnapshots(InvokeForTableRequest request) {
        LOGGER.info("Loading instance properties from config bucket {}", configBucketName);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, configBucketName);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3Client, dynamoClient);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(instanceProperties, s3Client, dynamoClient, new Configuration());
        TransactionLogSnapshotCreator snapshotCreator = new TransactionLogSnapshotCreator(
                instanceProperties, tablePropertiesProvider, stateStoreProvider, dynamoClient, new Configuration());
        try {
            snapshotCreator.run(request);
        } catch (Exception e) {
            LOGGER.error("Failed creating snapshots for tables {}", request.getTableIds(), e);
        }
    }
}
