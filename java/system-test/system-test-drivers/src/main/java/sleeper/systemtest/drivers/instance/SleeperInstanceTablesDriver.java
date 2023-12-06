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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.clients.status.update.AddTable;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.table.TableIndex;
import sleeper.core.util.PollWithRetries;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.REVISION_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_ID_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TABLE_NAME_INDEX_DYNAMO_TABLENAME;
import static sleeper.configuration.properties.instance.InstanceProperties.S3_INSTANCE_PROPERTIES_FILE;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;

public class SleeperInstanceTablesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceTablesDriver.class);

    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonDynamoDB dynamoDB;
    private final Configuration hadoopConfiguration;

    public SleeperInstanceTablesDriver(
            AmazonS3 s3, S3Client s3v2, AmazonDynamoDB dynamoDB, Configuration hadoopConfiguration) {
        this.s3 = s3;
        this.s3v2 = s3v2;
        this.dynamoDB = dynamoDB;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public void save(InstanceProperties instanceProperties, TableProperties deployedProperties) {
        tablePropertiesStore(instanceProperties).save(deployedProperties);
    }

    public void deleteAll(InstanceProperties instanceProperties) {
        clearBucket(instanceProperties.get(DATA_BUCKET));
        clearBucket(instanceProperties.get(CONFIG_BUCKET), key -> !S3_INSTANCE_PROPERTIES_FILE.equals(key));
        clearTables(
                instanceProperties.get(ACTIVE_FILEINFO_TABLENAME),
                instanceProperties.get(PARTITION_TABLENAME),
                instanceProperties.get(REVISION_TABLENAME),
                instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME),
                instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME));
        waitForTablesToEmpty(
                instanceProperties.get(TABLE_NAME_INDEX_DYNAMO_TABLENAME),
                instanceProperties.get(TABLE_ID_INDEX_DYNAMO_TABLENAME));
    }

    public void add(InstanceProperties instanceProperties, TableProperties properties) {
        try {
            new AddTable(s3, dynamoDB, instanceProperties, properties, hadoopConfiguration).run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties instanceProperties) {
        return new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
    }

    public StateStoreProvider createStateStoreProvider(InstanceProperties instanceProperties) {
        return new StateStoreProvider(dynamoDB, instanceProperties, hadoopConfiguration);
    }

    public TableIndex tableIndex(InstanceProperties instanceProperties) {
        return new DynamoDBTableIndex(instanceProperties, dynamoDB);
    }

    private TablePropertiesStore tablePropertiesStore(InstanceProperties instanceProperties) {
        return S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    }

    private void clearBucket(String bucketName) {
        clearBucket(bucketName, key -> true);
    }

    private void clearBucket(String bucketName, Predicate<String> deleteKey) {
        LOGGER.info("Clearing S3 bucket: {}", bucketName);
        s3v2.listObjectsV2Paginator(req -> req.bucket(bucketName))
                .stream().map(ListObjectsV2Response::contents)
                .map(foundObjects -> foundObjects.stream()
                        .map(S3Object::key)
                        .filter(deleteKey)
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .collect(Collectors.toUnmodifiableList()))
                .filter(not(List::isEmpty))
                .forEach(objectsToDelete -> s3v2.deleteObjects(req -> req
                        .bucket(bucketName)
                        .delete(del -> del.objects(objectsToDelete))));
    }

    private void clearTables(String... tableNames) {
        Stream.of(tableNames).parallel().forEach(this::clearTable);
    }

    private void clearTable(String tableName) {
        LOGGER.info("Clearing DynamoDB table: {}", tableName);
        TableDescription table = dynamoDB.describeTable(tableName).getTable();
        streamPagedResults(dynamoDB, new ScanRequest().withTableName(tableName))
                .flatMap(result -> result.getItems().stream())
                .parallel()
                .map(item -> getKey(item, table))
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
                    .pollUntil("table is empty", () ->
                            dynamoDB.scan(new ScanRequest().withTableName(tableName).withLimit(1))
                                    .getItems().isEmpty());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for table to empty: " + tableName, e);
        }
    }

    private static Map<String, AttributeValue> getKey(Map<String, AttributeValue> item, TableDescription table) {
        return table.getKeySchema().stream()
                .map(KeySchemaElement::getAttributeName)
                .map(name -> entry(name, item.get(name)))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
