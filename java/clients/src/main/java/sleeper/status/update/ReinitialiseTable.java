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
package sleeper.status.update;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.s3.S3StateStore;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
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
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final boolean splitPointStringsBase64Encoded;
    private final boolean deletePartitions;
    private final String instanceId;
    private final String tableName;
    private final String splitPointsFileLocation;

    public ReinitialiseTable(
            AmazonS3 s3Client,
            AmazonDynamoDB dynamoDBClient,
            String instanceId,
            String tableName,
            boolean deletePartitions,
            String splitPointsFileLocation,
            boolean splitPointStringsBase64Encoded) {
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
        this.deletePartitions = deletePartitions;
        this.splitPointStringsBase64Encoded = splitPointStringsBase64Encoded;
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.splitPointsFileLocation = splitPointsFileLocation;
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
            System.out.println("S3 State Store detected");
            isS3StateStore = true;
        } else {
            System.out.println("Dynamo DB State Store detected");
        }

        if (deletePartitions) {
            List<Object> splitPoints = calculateSplitPoints(tableProperties);
            deleteContentsOfDynamoDbTables(tableProperties, isS3StateStore);
            deleteObjectsInTableBucket(s3TableBucketName, isS3StateStore);

            System.out.println("Fully reinitialising table");
            StateStore statestore = new StateStoreFactory(dynamoDBClient, instanceProperties, conf)
                    .getStateStore(tableProperties);
            InitialiseStateStore initialiseStateStore =
                    new InitialiseStateStore(tableProperties, statestore, splitPoints);
            initialiseStateStore.run();
        } else {
            deleteContentsOfDynamoDbTables(tableProperties, isS3StateStore);
            deleteObjectsInTableBucket(s3TableBucketName, isS3StateStore);
            if (isS3StateStore) {
                System.out.println("Recreating files information file and adding it into the revisions table");
                S3StateStore s3StateStore = new S3StateStore(instanceProperties, tableProperties,
                        dynamoDBClient, conf);
                s3StateStore.setInitialFileInfos();
            }
        }
    }

    private void deleteObjectsInTableBucket(String s3TableBucketName, boolean isS3StateStore) {
        List<String> objectKeysForDeletion = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(s3TableBucketName)
                .withMaxKeys(100);
        ListObjectsV2Result result;

        System.out.println("Deleting all objects within partitions in the table's bucket");
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
        System.out.println("A total of " + totalObjectsDeleted + " objects were deleted");
    }

    private int deleteObjects(String bucketName, List<String> keys) {
        int successfulDeletes = 0;
        if (!keys.isEmpty()) {
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                    .withKeys(keys.toArray(new String[0]))
                    .withQuiet(false);
            DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
            successfulDeletes = delObjRes.getDeletedObjects().size();
            System.out.println(successfulDeletes + " objects successfully deleted from the "
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
        System.out.println("Deleting all items from " + dynamoTableName + " Dynamo DB Table");
        Map<String, AttributeValue> lastKeyEvaluated = null;
        int countOfDeletedItems = 0;
        do {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(dynamoTableName)
                    .withLimit(50)
                    .withExclusiveStartKey(lastKeyEvaluated);

            ScanResult result = dynamoDBClient.scan(scanRequest);
            for (Map<String, AttributeValue> item : result.getItems()) {
                if (dynamoTableName.contains(tableProperties.get(PARTITION_TABLENAME))) {
                    dynamoDBClient.deleteItem(
                            new DeleteItemRequest(
                                    dynamoTableName,
                                    Collections.singletonMap(PARTITION_ID, item.get(PARTITION_ID))));

                } else {
                    dynamoDBClient.deleteItem(
                            new DeleteItemRequest(
                                    dynamoTableName,
                                    Collections.singletonMap(FILE_NAME, item.get(FILE_NAME))));
                }
                countOfDeletedItems++;
            }
            lastKeyEvaluated = result.getLastEvaluatedKey();
        } while (lastKeyEvaluated != null);

        System.out.println(countOfDeletedItems + " items successfully deleted from " + dynamoTableName + " Dynamo DB Table");
    }

    private void deleteRelevantS3StateStoreRevisionInfo(String dynamoTableName) {
        System.out.println("Deleting files info items from " + dynamoTableName + " Dynamo DB Table");
        Map<String, AttributeValue> lastKeyEvaluated = null;
        int countOfDeletedItems = 0;
        do {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(dynamoTableName)
                    .withLimit(50)
                    .withExclusiveStartKey(lastKeyEvaluated);

            ScanResult result = dynamoDBClient.scan(scanRequest);
            for (Map<String, AttributeValue> item : result.getItems()) {
                if (deletePartitions) {
                    dynamoDBClient.deleteItem(
                            new DeleteItemRequest(
                                    dynamoTableName,
                                    Collections.singletonMap(REVISION_ID_KEY, item.get(REVISION_ID_KEY))));
                    countOfDeletedItems++;
                } else {
                    if (item.get(REVISION_ID_KEY).toString().contains(CURRENT_FILES_REVISION_ID_KEY)) {
                        dynamoDBClient.deleteItem(
                                new DeleteItemRequest(
                                        dynamoTableName,
                                        Collections.singletonMap(REVISION_ID_KEY, item.get(REVISION_ID_KEY))));
                        countOfDeletedItems++;
                    }
                }

            }
            lastKeyEvaluated = result.getLastEvaluatedKey();
        } while (lastKeyEvaluated != null);

        System.out.println(countOfDeletedItems + " items successfully deleted from " + dynamoTableName + " Dynamo DB Table");
    }

    private List<Object> calculateSplitPoints(TableProperties tableProperties) throws IOException {
        List<Object> splitPoints = null;
        if (splitPointsFileLocation != null && !splitPointsFileLocation.isEmpty()) {
            splitPoints = new ArrayList<>();

            PrimitiveType rowKey1Type = tableProperties.getSchema().getRowKeyTypes().get(0);
            List<String> lines = new ArrayList<>();
            try (InputStream inputStream = new FileInputStream(splitPointsFileLocation);
                 Reader tempReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(tempReader)) {
                String lineFromFile = reader.readLine();
                while (null != lineFromFile) {
                    lines.add(lineFromFile);
                    lineFromFile = reader.readLine();
                }
            }
            for (String line : lines) {
                if (rowKey1Type instanceof IntType) {
                    splitPoints.add(Integer.parseInt(line));
                } else if (rowKey1Type instanceof LongType) {
                    splitPoints.add(Long.parseLong(line));
                } else if (rowKey1Type instanceof StringType) {
                    if (splitPointStringsBase64Encoded) {
                        byte[] encodedString = Base64.decodeBase64(line);
                        splitPoints.add(new String(encodedString, StandardCharsets.UTF_8));
                    } else {
                        splitPoints.add(line);
                    }
                } else if (rowKey1Type instanceof ByteArrayType) {
                    splitPoints.add(Base64.decodeBase64(line));
                } else {
                    throw new RuntimeException("Unknown key type " + rowKey1Type);
                }
            }
            System.out.println("Read " + splitPoints.size() + " split points from file");
        }
        return splitPoints;
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 5) {
            throw new IllegalArgumentException("Usage: <instance id> <table name> " +
                    "<optional_delete_partitions_true_or_false> " +
                    "<optional_split_points_file_location> " +
                    "<optional_split_points_file_is_base64_encoded_true_or_false>");
        }

        System.out.println("\nIf you continue all data will be deleted in the table\n" +
                "Including the partitions data as well, if you have chosen that option\n");
        String choice = System.console().readLine("Are you sure you want to delete the data and " +
                "reinitialise this table?\nPlease enter Y or N: ");
        if (!choice.equalsIgnoreCase("y")) {
            System.exit(0);
        }
        AmazonS3 amazonS3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();

        try {
            ReinitialiseTable reinitialiseTable;
            if (args.length == 2) {
                reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1], false,
                        null, false);
                reinitialiseTable.run();
            } else {
                if (args.length == 3) {
                    if (!Boolean.parseBoolean(args[2])) {
                        reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                false, null, false);
                        reinitialiseTable.run();
                    } else {
                        reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                true, null, false);
                        reinitialiseTable.run();
                    }
                }
                if (args.length == 4) {
                    if (!Boolean.parseBoolean(args[2])) {
                        reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                false, null, false);
                        reinitialiseTable.run();
                    } else {
                        reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                true, args[3], false);
                        reinitialiseTable.run();
                    }
                }
                if (args.length == 5) {
                    if (!Boolean.parseBoolean(args[2])) {
                        reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                false, null, false);
                        reinitialiseTable.run();
                    } else {
                        if (!Boolean.parseBoolean(args[4])) {
                            reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                    true, args[3], false);
                            reinitialiseTable.run();
                        } else {
                            reinitialiseTable = new ReinitialiseTable(amazonS3, dynamoDBClient, args[0], args[1],
                                    true, args[3], true);
                            reinitialiseTable.run();
                        }
                    }
                }
            }
            System.out.println("Table reinitialised successfully");
        } catch (RuntimeException | IOException | StateStoreException e) {
            System.out.println("\nAn Error occurred while trying to reinitialise the table. " +
                    "The error message is as follows:\n\n" + e.getMessage()
                    + "\n\nCause:" + e.getCause());
        }
        amazonS3.shutdown();
        dynamoDBClient.shutdown();
    }
}
