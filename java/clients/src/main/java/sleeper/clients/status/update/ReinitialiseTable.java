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
package sleeper.clients.status.update;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.s3.S3StateStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.FILE_NAME;
import static sleeper.statestore.dynamodb.DynamoDBStateStore.PARTITION_ID;
import static sleeper.statestore.s3.S3StateStore.CURRENT_FILES_REVISION_ID_KEY;
import static sleeper.statestore.s3.S3StateStore.REVISION_ID_KEY;

/**
 * A utility class to reinitialise a table by first deleting the table's contents
 * and the state store items related to them. Then the state store for the table
 * is reinitialised.
 */
public class ReinitialiseTable {
    private static final java.util.logging.Logger LOGGER = LoggerFactory.getLogger(ReinitialiseTable.class);
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final boolean deletePartitions;
    private final String instanceId;
    private final String tableName;

    public ReinitialiseTable(
            AmazonS3 s3Client,
            AmazonDynamoDB dynamoDBClient,
            String instanceId,
            String tableName,
            boolean deletePartitions) {
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.deletePartitions = deletePartitions;
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        if (instanceId.isEmpty() || tableName.isEmpty()) {
            throw new IllegalArgumentException("You have tried to create a ReinitialiseTable class with " +
                    "an empty String in the instance id or table name. These must not be empty.");
        }
    }

    void run() throws IOException, StateStoreException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
        TablePropertiesProvider tablePropertiesProvider =
                new TablePropertiesProvider(s3Client, instanceProperties);
        TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);

        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", DefaultAWSCredentialsProviderChain.class.getName());

        String instanceTableName = instanceProperties.get(ID) + "-table-" + tableName;
        String s3TableBucketName = "sleeper-" + instanceTableName;

        boolean isS3StateStore = false;
        if (tableProperties.get(STATESTORE_CLASSNAME).equals("sleeper.statestore.s3.S3StateStore")) {
            LOGGER.fine("S3 State Store detected");
            isS3StateStore = true;
        } else {
            LOGGER.fine("Dynamo DB State Store detected");
        }

        if (deletePartitions) {
            deleteContentsOfDynamoDbTables(tableProperties, isS3StateStore);
            deleteObjectsInTableBucket(s3TableBucketName, isS3StateStore);

            LOGGER.fine("Fully reinitialising table");
            StateStore statestore = new StateStoreFactory(dynamoDBClient, instanceProperties, conf)
                    .getStateStore(tableProperties);
            initialiseStateStore(tableProperties, statestore);
        } else {
            deleteContentsOfDynamoDbTables(tableProperties, isS3StateStore);
            deleteObjectsInTableBucket(s3TableBucketName, isS3StateStore);
            if (isS3StateStore) {
                LOGGER.fine("Recreating files information file and adding it into the revisions table");
                S3StateStore s3StateStore = new S3StateStore(instanceProperties, tableProperties,
                        dynamoDBClient, conf);
                s3StateStore.setInitialFileInfos();
            }
        }
    }

    protected void initialiseStateStore(TableProperties tableProperties, StateStore stateStore) throws IOException, StateStoreException {
        InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(tableProperties, stateStore, Collections.emptyList()).run();
    }

    private void deleteObjectsInTableBucket(String s3TableBucketName, boolean isS3StateStore) {
        List<String> objectKeysForDeletion = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(s3TableBucketName)
                .withMaxKeys(100);
        ListObjectsV2Result result;

        LOGGER.fine("Deleting all objects within partitions in the table's bucket");
        int totalObjectsDeleted = 0;
        do {
            objectKeysForDeletion.clear();
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                String objectKey = objectSummary.getKey();
                if (objectKey.matches("partition.*/.*")) {
                    objectKeysForDeletion.add(objectSummary.getKey());
                }
                if (isS3StateStore) {
                    if (deletePartitions && objectKey.matches("statestore/.*")) {
                        objectKeysForDeletion.add(objectSummary.getKey());
                    } else if (objectKey.matches("statestore/files/.*")) {
                        objectKeysForDeletion.add(objectSummary.getKey());
                    }
                }
            }
            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
            totalObjectsDeleted += deleteObjects(s3TableBucketName, objectKeysForDeletion);
        } while (result.isTruncated());
        LOGGER.fine("A total of " + totalObjectsDeleted + " objects were deleted");
    }

    private int deleteObjects(String bucketName, List<String> keys) {
        int successfulDeletes = 0;
        if (!keys.isEmpty()) {
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                    .withKeys(keys.toArray(new String[0]))
                    .withQuiet(false);
            DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
            successfulDeletes = delObjRes.getDeletedObjects().size();
            LOGGER.fine(successfulDeletes + " objects successfully deleted from the "
                    + bucketName + " S3 bucket");
        }
        return successfulDeletes;
    }

    private void deleteContentsOfDynamoDbTables(TableProperties tableProperties, boolean isS3StateStore) {
        if (isS3StateStore) {
            deleteRelevantS3StateStoreRevisionInfo(tableProperties.get(REVISION_TABLENAME));
        } else {
            deleteAllDynamoTableItems(tableProperties.get(ACTIVE_FILEINFO_TABLENAME), tableProperties);
            deleteAllDynamoTableItems(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME), tableProperties);
            if (deletePartitions) {
                deleteAllDynamoTableItems(tableProperties.get(PARTITION_TABLENAME), tableProperties);
            }
        }
    }

    private void deleteAllDynamoTableItems(String dynamoTableName, TableProperties tableProperties) {
        LOGGER.fine("Deleting all items from " + dynamoTableName + " Dynamo DB Table");
        long countOfDeletedItems = streamPagedItems(dynamoDBClient,
                new ScanRequest()
                        .withTableName(dynamoTableName)
                        .withLimit(50))
                .map(item -> {
                    Map<String, AttributeValue> deleteKey;
                    if (dynamoTableName.contains(tableProperties.get(PARTITION_TABLENAME))) {
                        deleteKey = Collections.singletonMap(PARTITION_ID, item.get(PARTITION_ID));
                    } else {
                        deleteKey = Collections.singletonMap(FILE_NAME, item.get(FILE_NAME));
                    }
                    return dynamoDBClient.deleteItem(
                            new DeleteItemRequest(dynamoTableName, deleteKey));
                }).count();

        LOGGER.fine(countOfDeletedItems + " items successfully deleted from " + dynamoTableName + " Dynamo DB Table");
    }

    private void deleteRelevantS3StateStoreRevisionInfo(String dynamoTableName) {
        LOGGER.fine("Deleting files info items from " + dynamoTableName + " Dynamo DB Table");
        long countOfDeletedItems = streamPagedItems(dynamoDBClient,
                new ScanRequest()
                        .withTableName(dynamoTableName)
                        .withLimit(50))
                .filter(item -> deletePartitions
                        || item.get(REVISION_ID_KEY).toString().contains(CURRENT_FILES_REVISION_ID_KEY))
                .map(item -> dynamoDBClient.deleteItem(
                        new DeleteItemRequest(
                                dynamoTableName,
                                Collections.singletonMap(REVISION_ID_KEY, item.get(REVISION_ID_KEY)))))
                .count();

        LOGGER.fine(countOfDeletedItems + " items successfully deleted from " + dynamoTableName + " Dynamo DB Table");
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Usage: <instance id> <table name> <optional_delete_partitions_true_or_false>");
        }
        String instanceId = args[0];
        String tableName = args[1];
        boolean deletePartitions = args.length == 2 ? false : Boolean.parseBoolean(args[2]);

        LOGGER.info("If you continue all data will be deleted in the table.");
        if (deletePartitions) {
            LOGGER.info("The metadata about the partitions will be deleted and the "
                + "table will be reset to consist of one root partition.");
        } else {
            LOGGER.info("The metadata about the partitions will not be deleted.");
        }
        String choice = System.console().readLine("Are you sure you want to delete the data and " +
                "reinitialise this table?\nPlease enter Y or N: ");
        if (!choice.equalsIgnoreCase("y")) {
            System.exit(0);
        }
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        try {
            ReinitialiseTable reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, instanceId, tableName, deletePartitions);
            reinitialiseTable.run();
            LOGGER.info("Table reinitialised successfully");
        } catch (RuntimeException | IOException | StateStoreException e) {
            LOGGER.severe("\nAn Error occurred while trying to reinitialise the table. " +
                    "The error message is as follows:\n\n" + e.getMessage()
                    + "\n\nCause:" + e.getCause());
        }
        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
