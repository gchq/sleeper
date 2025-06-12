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
package sleeper.statestore.lambda.snapshot;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configurationv2.properties.S3InstanceProperties;
import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.util.LoggedDuration;
import sleeper.invoke.tables.InvokeForTables;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;

import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.STATESTORE_CLASSNAME;

/**
 * A lambda that periodically creates batches of tables and sends them to a queue to delete old transaction log
 * snapshots.
 */
public class TransactionLogSnapshotDeletionTriggerLambda implements RequestHandler<ScheduledEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionLogSnapshotCreationTriggerLambda.class);

    private final InstanceProperties instanceProperties;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final SqsClient sqsClient;

    public TransactionLogSnapshotDeletionTriggerLambda() {
        this.s3Client = S3Client.create();
        this.dynamoClient = DynamoDbClient.create();
        this.sqsClient = SqsClient.create();
        String configBucketName = System.getenv(CONFIG_BUCKET.toEnvironmentVariable());
        instanceProperties = S3InstanceProperties.loadFromBucket(s3Client, configBucketName);
    }

    @Override
    public Void handleRequest(ScheduledEvent event, Context context) {
        Instant startTime = Instant.now();
        LOGGER.info("Lambda triggered at {}, started at {}", event.getTime(), startTime);
        String queueUrl = instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_DELETION_QUEUE_URL);
        InvokeForTables.sendOneMessagePerTable(sqsClient, queueUrl, streamOnlineTransactionLogTables());

        Instant finishTime = Instant.now();
        LOGGER.info("Lambda finished at {} (ran for {})", finishTime, LoggedDuration.withFullOutput(startTime, finishTime));
        return null;
    }

    private Stream<TableStatus> streamOnlineTransactionLogTables() {
        TableIndex tableIndex = new DynamoDBTableIndex(instanceProperties, dynamoClient);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties,
                S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient), Instant::now);
        return tableIndex.streamOnlineTables()
                .filter(tableStatus -> DynamoDBTransactionLogStateStore.class.getSimpleName()
                        .equals(tablePropertiesProvider.getById(tableStatus.getTableUniqueId()).get(STATESTORE_CLASSNAME)));
    }
}
