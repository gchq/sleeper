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
package sleeper.bulkexport.planner;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.bulkexport.core.model.BulkExportQueryValidationException;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.transactionlog.TransactionLogStateStore;
import sleeper.core.util.ObjectFactoryException;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.statestore.transactionlog.DynamoDBTransactionLogStateStore;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithNoReferences;
import static sleeper.core.statestore.AllReferencesToAFileTestHelper.fileWithReferences;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class SqsBulkExportProcessorLambdaIT extends LocalStackTestBase {

    private static final Schema KEY_VALUE_SCHEMA = Schema.builder()
            .rowKeyFields(new Field("key", new StringType()))
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();

    private InstanceProperties instanceProperties;
    private SqsBulkExportProcessorLambda sqsBulkExportProcessorLambda;
    private TableProperties tableProperties;

    @BeforeEach
    public void setUp() throws ObjectFactoryException, IOException {
        creatInstanceProperties();
        tableProperties = createTestTableProperties(instanceProperties, KEY_VALUE_SCHEMA);
        DynamoDBTableIndexCreator.create(dynamoClient, instanceProperties);
        S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient).save(tableProperties);
        setupTransactionLogStateStore(tableProperties);
        sqsBulkExportProcessorLambda = new SqsBulkExportProcessorLambda(sqsClient, s3Client, dynamoClient, instanceProperties.get(CONFIG_BUCKET));
    }

    @Test
    public void testProcessorLambdaProducesLeafPartitionMessages() {
        // Given
        String tableName = tableProperties.get(TABLE_NAME);
        SQSEvent event = createEvent("{\"tableName\":\"" + tableName + "\"}");

        // When
        sqsBulkExportProcessorLambda.handleRequest(event, null);

        // Then
        List<ReceiveMessageResult> results = new ArrayList<>();
        ReceiveMessageResult result;
        do {
            result = sqsClient.receiveMessage(new ReceiveMessageRequest(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL)));
            if (result.getMessages().size() > 0) {
                results.add(result);
            }
        } while (result.getMessages().size() > 0);
        assertThat(results).hasSize(2);
    }

    @Test
    public void testMalformedJsonThrowsException() {
        // Given
        SQSEvent event = createEvent("not json");

        // When Then
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");
    }

    @Test
    public void testValidationExceptionThrowsException() {
        // Given
        SQSEvent event = createEvent("{\"invalidField\":\"value\"}");

        // When Then
        assertThatThrownBy(() -> sqsBulkExportProcessorLambda.handleRequest(event, null))
                .isInstanceOf(BulkExportQueryValidationException.class)
                .hasMessageContaining("Query validation failed: tableId or tableName field must be provided");
    }

    private SQSEvent createEvent(String messageBody) {
        SQSEvent event = new SQSEvent();
        SQSEvent.SQSMessage message = new SQSEvent.SQSMessage();
        message.setBody(messageBody);
        event.setRecords(Collections.singletonList(message));
        return event;
    }

    private void creatInstanceProperties() {
        instanceProperties = createTestInstanceProperties();
        instanceProperties.set(BULK_EXPORT_QUEUE_URL, sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL,
                sqsClient.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));
        S3InstanceProperties.saveToS3(s3Client, instanceProperties);
    }

    private TransactionLogStateStore setupTransactionLogStateStore(TableProperties tableProperties) throws IOException {
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        TransactionLogStateStore transctionLogStateStore = DynamoDBTransactionLogStateStore.builderFrom(
                instanceProperties, tableProperties, dynamoClient, s3Client, hadoopConf).build();

        setupPartitionsAndAddFiles(transctionLogStateStore);

        return transctionLogStateStore;
    }

    private void setupPartitionsAndAddFiles(StateStore stateStore) throws IOException {
        //  - Set partitions
        PartitionTree tree = new PartitionsBuilder(
                KEY_VALUE_SCHEMA)
                .rootFirst("root")
                .splitToNewChildren("root", "0---eee", "eee---zzz", "eee")
                .buildTree();
        update(stateStore).initialise(tree.getAllPartitions());

        //  - Create 3 fake files, 2 of which have references
        String file1 = "file1.parquet";
        String file2 = "file2.parquet";
        String file3 = "file3.parquet";

        FileReference fileReference1 = createFileReference(file1, "root");
        FileReference fileReference2 = createFileReference(file2, "root");

        //  - Update Dynamo state store with details of files
        update(stateStore).addFilesWithReferences(List.of(
                fileWithReferences(List.of(fileReference1)),
                fileWithReferences(List.of(fileReference2)),
                fileWithNoReferences(file3)));
    }

    private FileReference createFileReference(String filename, String partitionId) {
        return FileReference.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(100L)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
    }
}
