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
package sleeper.bulkexport.taskexecution;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuery;
import sleeper.bulkexport.core.model.BulkExportLeafPartitionQuerySerDe;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordReader;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_S3_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class ECSBulkExportTaskRunnerLocalStackIT extends LocalStackTestBase {
    private final Field field = new Field("key", new IntType());
    private final Schema schema = Schema.builder()
            .rowKeyFields(field)
            .valueFields(new Field("value1", new StringType()), new Field("value2", new StringType()))
            .build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final PartitionTree partitions = new PartitionsBuilder(tableProperties)
            .rootFirst("root")
            .splitToNewChildren("root", "L", "R", 1000)
            .buildTree();
    private final BulkExportLeafPartitionQuerySerDe serDe = new BulkExportLeafPartitionQuerySerDe(schema);

    @BeforeEach
    void setUp() {
        tableProperties.set(TABLE_ID, "t-id");
        instanceProperties.setNumber(BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS, 1);
        instanceProperties.setNumber(BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS, 1);
        instanceProperties.set(BULK_EXPORT_S3_BUCKET, UUID.randomUUID().toString());
        createBucket(instanceProperties.get(BULK_EXPORT_S3_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
        update(stateStore()).initialise(partitions.getAllPartitions());
    }

    @Test
    public void shouldRunOneBulkExportSubQuery() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        Record record1 = new Record(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Record record2 = new Record(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(record1, record2));
        BulkExportLeafPartitionQuery firstQuery = null;
        for (int i = 0; i < 2; i++) {
            BulkExportLeafPartitionQuery query = createQueryWithIdsAndFiles(String.format("e-id-%d", i), String.format("se-id-%d", i), file);
            send(query);
            if (i == 0) {
                // This needed for the assertion
                firstQuery = query;
            }
        }

        // When
        runTask();

        // Then
        assertThat(readOutputFile(firstQuery)).containsExactly(record1, record2);
        assertThat(getMessagesFromDlq().size()).isEqualTo(0);
        assertThat(getMessagesFromQueue().size()).isEqualTo(1);
    }

    @Test
    public void shouldReturnMessageToQueueAfterFailure() throws Exception {
        // Given
        String messageString = "This will cause a failure!";
        configureJobQueuesWithMaxReceiveCount(2);
        send(messageString);

        // When
        assertThatThrownBy(() -> runTask())
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");

        // Then
        assertThat(getMessagesFromDlq().size()).isEqualTo(0);
        List<String> messages = getMessagesFromQueue();
        assertThat(messages)
                .size().isEqualTo(1);
        assertThat(messages.get(0)).contains(messageString);
    }

    @Test
    public void shouldHandleExceptionInProcessingAndSendToDlq() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        Record record1 = new Record(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Record record2 = new Record(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(record1, record2));
        BulkExportLeafPartitionQuery query = createQueryWithIdsFilesAndBrokenPartition("e-id", "se-id", file);

        send(query);

        // When
        assertThatThrownBy(() -> runTask())
                .hasMessageContaining("Partition not found: NO-ID");

        runTask();

        // Then
        List<String> messages = getMessagesFromDlq();
        assertThat(messages)
                .size().isEqualTo(1);

        BulkExportLeafPartitionQuery receivedQuery = serDe.fromJson(messages.get(0));
        assertThat(receivedQuery).isEqualTo(query);
        assertThat(getMessagesFromQueue().size()).isEqualTo(0);

    }

    @Test
    public void shouldMoveMessageToDlqAfterTwoFailures() throws Exception {
        // Given
        String messageString = "This will cause a failure!";
        configureJobQueuesWithMaxReceiveCount(1);
        send(messageString);

        // When
        // The task needs to be run twice for it to be moved to the DLQ
        assertThatThrownBy(() -> runTask())
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");

        runTask();

        // Then
        List<String> messages = getMessagesFromDlq();
        assertThat(messages)
                .size().isEqualTo(1);
        assertThat(messages.get(0)).contains(messageString);
        assertThat(getMessagesFromQueue().size()).isEqualTo(0);
    }

    @Test
    public void shouldProcessNoMessages() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(2);

        // When
        runTask();

        // Then
        assertThat(getMessagesFromQueue()).isEmpty();
        assertThat(getMessagesFromDlq().size()).isEqualTo(0);
    }

    private BulkExportLeafPartitionQuery createQueryWithIdsAndFiles(
            String exportId, String subExportId, FileReference... files) {
        String partitionId = files[0].getPartitionId();
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .exportId(exportId)
                .subExportId(subExportId)
                .regions(List.of())
                .leafPartitionId(partitionId)
                .partitionRegion(partitions.getPartition(partitionId).getRegion())
                .files(Stream.of(files).map(FileReference::getFilename).toList())
                .build();
    }

    private BulkExportLeafPartitionQuery createQueryWithIdsFilesAndBrokenPartition(
            String exportId, String subExportId, FileReference... files) {
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .exportId(exportId)
                .subExportId(subExportId)
                .regions(List.of())
                .leafPartitionId("NO-ID")
                .partitionRegion(partitions.getPartition(files[0]
                        .getPartitionId()).getRegion())
                .files(Stream.of(files).map(FileReference::getFilename).toList())
                .build();
    }

    private void runTask() throws Exception {
        ECSBulkExportTaskRunner.runECSBulkExportTaskRunner(
                instanceProperties, new FixedTablePropertiesProvider(tableProperties), sqsClient, s3Client, dynamoClient, hadoopConf);
    }

    private StateStore stateStore() {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private FileReference addPartitionFile(String partitionId, String name, List<Record> records) {
        FileReference reference = fileFactory().partitionFile(partitionId, name, records.size());
        Path path = new Path(reference.getFilename());
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, tableProperties, hadoopConf)) {
            for (Record record : records) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        update(stateStore()).addFile(reference);
        return reference;
    }

    private List<Record> readOutputFile(BulkExportLeafPartitionQuery query) {
        Path path = new Path(query.getOutputFile(instanceProperties));
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                new ParquetRecordReader.Builder(path, schema).withConf(hadoopConf).build())) {
            List<Record> records = new ArrayList<>();
            reader.forEachRemaining(records::add);
            return records;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FileReferenceFactory fileFactory() {
        return FileReferenceFactory.from(instanceProperties, tableProperties, partitions);
    }

    private void send(BulkExportLeafPartitionQuery query) {
        BulkExportLeafPartitionQuerySerDe serDe = new BulkExportLeafPartitionQuerySerDe(
                new FixedTablePropertiesProvider(tableProperties));
        String messageBody = serDe.toJson(query);
        send(messageBody);
    }

    private void send(String messageBody) {
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL))
                .messageBody(messageBody)
                .build());
    }

    private List<String> getMessagesFromDlq() {
        return getMessagesFromSuppliedQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL));
    }

    private List<String> getMessagesFromQueue() {
        return getMessagesFromSuppliedQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL));
    }

    private List<String> getMessagesFromSuppliedQueue(String url) {
        List<String> messageBodies = new ArrayList<>();
        while (true) {
            ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .queueUrl(url)
                    .waitTimeSeconds(2)
                    .maxNumberOfMessages(10)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(request).messages();
            if (messages.isEmpty()) {
                break;
            }
            messageBodies.addAll(messages.stream().map(Message::body).toList());
        }
        return messageBodies;
    }

    private void configureJobQueuesWithMaxReceiveCount(int maxReceiveCount) {
        String jobQueueUrl = createSqsQueueGetUrl();
        String jobDlqUrl = createSqsQueueGetUrl();
        String jobDlqArn = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(jobDlqUrl)
                .attributeNames(List.of(QueueAttributeName.QUEUE_ARN))
                .build())
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);

        String visibilityTimeoutSeconds = instanceProperties.get(BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS);

        Map<QueueAttributeName, String> attributes = Map.of(
                QueueAttributeName.REDRIVE_POLICY, "{\"maxReceiveCount\":\"" + maxReceiveCount + "\", \"deadLetterTargetArn\":\"" + jobDlqArn + "\"}",
                QueueAttributeName.VISIBILITY_TIMEOUT, visibilityTimeoutSeconds);

        sqsClient.setQueueAttributes(SetQueueAttributesRequest.builder()
                .queueUrl(jobQueueUrl)
                .attributes(attributes)
                .build());
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL, jobQueueUrl);
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL, jobDlqUrl);
    }
}
