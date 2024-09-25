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
package sleeper.statestore.snapshot;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.invoke.tables.InvokeForTables;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;

import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;

/**
 * A lambda that periodically creates batches of tables and sends them to a queue to create transaction log snapshots.
 */
public class TransactionLogSnapshotCreationTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreationTriggerLambda.class);

    private final InstanceProperties instanceProperties;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoClient;
    private final AmazonSQS sqsClient;

    public TransactionLogSnapshotCreationTriggerLambda() {
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.dynamoClient = AmazonDynamoDBClientBuilder.defaultClient();
        this.sqsClient = AmazonSQSClientBuilder.defaultClient();
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        String queueUrl = instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_CREATION_QUEUE_URL);
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, streamOnlineTransactionLogTables());

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

    private Stream<TableStatus> streamOnlineTransactionLogTables() {
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties,
                S3TableProperties.getStore(instanceProperties, s3Client, dynamoClient), Instant::now);
        return tableIndex.streamOnlineTables()
                .filter(tableStatus -> DynamoDBTransactionLogStateStore.class.getName()
                        .equals(tablePropertiesProvider.getById(tableStatus.getTableUniqueId()).get(STATESTORE_CLASSNAME)));
    }
}
