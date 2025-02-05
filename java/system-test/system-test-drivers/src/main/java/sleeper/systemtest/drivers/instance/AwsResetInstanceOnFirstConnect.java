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
package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.deploy.SqsQueues;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.snapshots.DynamoDBTransactionLogSnapshotMetadataStore;
import sleeper.systemtest.drivers.util.AwsDrainSqsQueue;
import sleeper.systemtest.drivers.util.SystemTestClients;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.FILE_REFERENCE_COUNT_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TABLE_ONLINE_INDEX_DYNAMO_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_FILES_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_PARTITIONS_TABLENAME;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;

public class AwsResetInstanceOnFirstConnect {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsResetInstanceOnFirstConnect.class);
    private final S3Client s3;
    private final AmazonDynamoDB dynamoDB;
    private final SqsClient sqs;

    public AwsResetInstanceOnFirstConnect(SystemTestClients clients) {
        this.s3 = clients.getS3V2();
        this.dynamoDB = clients.getDynamoDB();
        this.sqs = clients.getSqsV2();
    }

    public void reset(InstanceProperties instanceProperties) {
        deleteAllTables(instanceProperties);
        drainAllQueues(instanceProperties);
    }

    private void deleteAllTables(InstanceProperties instanceProperties) {
        clearBucket(instanceProperties.get(DATA_BUCKET));
        clearBucket(instanceProperties.get(CONFIG_BUCKET), key -> !S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE.equals(key));
        clearTable(instanceProperties.get(ACTIVE_FILES_TABLENAME), DynamoDBStateStore.TABLE_ID, DynamoDBStateStore.PARTITION_ID_AND_FILENAME);
        clearTable(instanceProperties.get(FILE_REFERENCE_COUNT_TABLENAME), DynamoDBStateStore.TABLE_ID, DynamoDBStateStore.FILE_NAME);
        clearTable(instanceProperties.get(PARTITION_TABLENAME), DynamoDBStateStore.TABLE_ID, DynamoDBStateStore.PARTITION_ID);
        clearTable(instanceProperties.get(REVISION_TABLENAME), S3StateStore.TABLE_ID, S3StateStore.REVISION_ID_KEY);
        clearTable(instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME), DynamoDBTableIndex.TABLE_NAME_FIELD);
        clearTable(instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME), DynamoDBTableIndex.TABLE_ID_FIELD);
        clearTable(instanceProperties.get(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME),
                DynamoDBTableIndex.TABLE_ONLINE_FIELD, DynamoDBTableIndex.TABLE_NAME_FIELD);
        clearTable(instanceProperties.get(TRANSACTION_LOG_FILES_TABLENAME),
                DynamoDBTransactionLogStateStore.TABLE_ID, DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER);
        clearTable(instanceProperties.get(TRANSACTION_LOG_PARTITIONS_TABLENAME),
                DynamoDBTransactionLogStateStore.TABLE_ID, DynamoDBTransactionLogStateStore.TRANSACTION_NUMBER);
        clearTable(instanceProperties.get(TRANSACTION_LOG_ALL_SNAPSHOTS_TABLENAME),
                DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID_AND_SNAPSHOT_TYPE, DynamoDBTransactionLogSnapshotMetadataStore.TRANSACTION_NUMBER);
        clearTable(instanceProperties.get(TRANSACTION_LOG_LATEST_SNAPSHOTS_TABLENAME), DynamoDBTransactionLogSnapshotMetadataStore.TABLE_ID);
        waitForTablesToEmpty(
                instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME),
                instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME),
                instanceProperties.get(TABLE_ONLINE_INDEX_DYNAMO_TABLENAME));
    }

    private void drainAllQueues(InstanceProperties instanceProperties) {
        List<String> queueUrls = SqsQueues.ALL_QUEUE_URL_PROPERTIES.stream()
                .map(instanceProperties::get)
                .filter(queueUrl -> queueUrl != null)
                .toList();
        queueUrls.stream().parallel().forEach(queueUrl -> AwsDrainSqsQueue.emptyQueueForWholeInstance(sqs, queueUrl));
    }

    private void clearBucket(String bucketName) {
        clearBucket(bucketName, key -> true);
    }

    private void clearBucket(String bucketName, Predicate<String> deleteKey) {
        LOGGER.info("Clearing S3 bucket: {}", bucketName);
        s3.listObjectsV2Paginator(req -> req.bucket(bucketName))
                .stream().map(ListObjectsV2Response::contents)
                .map(foundObjects -> foundObjects.stream()
                        .map(S3Object::key)
                        .filter(deleteKey)
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .collect(Collectors.toUnmodifiableList()))
                .filter(not(List::isEmpty))
                .forEach(objectsToDelete -> s3.deleteObjects(req -> req
                        .bucket(bucketName)
                        .delete(del -> del.objects(objectsToDelete))));
    }

    private void clearTable(String tableName, String... keyFields) {
        LOGGER.info("Clearing DynamoDB table: {}", tableName);
        streamPagedResults(dynamoDB, new ScanRequest().withTableName(tableName))
                .flatMap(result -> result.getItems().stream())
                .parallel()
                .map(item -> getKey(item, List.of(keyFields)))
                .forEach(key -> dynamoDB.deleteItem(new DeleteItemRequest()
                        .withTableName(tableName)
                        .withKey(key)));
        LOGGER.info("Cleared DynamoDB table: {}", tableName);
    }

    private void waitForTablesToEmpty(String... tableNames) {
        for (String tableName : tableNames) {
            waitForTableToEmpty(tableName);
        }
    }

    private void waitForTableToEmpty(String tableName) {
        LOGGER.info("Waiting for DynamoDB table to empty: {}", tableName);
        try {
            PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(1), Duration.ofSeconds(30))
                    .pollUntil("table is empty", () -> dynamoDB.scan(new ScanRequest().withTableName(tableName).withLimit(1))
                            .getItems().isEmpty());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for table to empty: " + tableName, e);
        }
    }

    private static Map<String, AttributeValue> getKey(Map<String, AttributeValue> item, List<String> keyFields) {
        return keyFields.stream()
                .map(name -> Map.entry(name, item.get(name)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
