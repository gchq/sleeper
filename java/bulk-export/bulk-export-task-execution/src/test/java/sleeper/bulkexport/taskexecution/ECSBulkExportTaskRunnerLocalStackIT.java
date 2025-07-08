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
import org.junit.jupiter.api.Disabled;
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
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS;
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

    @BeforeEach
    void setUp() {
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL, createSqsQueueGetUrl());
        instanceProperties.set(LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL, createSqsQueueGetUrl());
        tableProperties.set(TABLE_ID, "t-id");
        instanceProperties.setNumber(BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS, 0);
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
        BulkExportLeafPartitionQuery query1 = createQueryWithIdsAndFiles("e-1", "se-1", file);
        BulkExportLeafPartitionQuery query2 = createQueryWithIdsAndFiles("e-2", "se-2", file);
        send(query1);
        send(query2);

        // When
        runTask();

        // Then
        assertThat(readOutputFile(query1)).containsExactly(record1, record2);
        assertThat(getMessagesFromDlq()).isEmpty();
        assertThat(getJobsFromQueue()).containsExactly(query2);
    }

    @Test
    @Disabled("TODO")
    // This fails because the message isn't returned immediately to the queue, instead it's left to time out with the
    // standard queue visibility timeout. It seems like ideally we would prefer this to immediately go to the dead
    // letter queue, since it's unreadable.
    public void shouldReturnMessageToQueueAfterFailure() throws Exception {
        // Given
        String messageString = "This will cause a failure!";
        configureJobQueuesWithMaxReceiveCount(2);
        send(messageString);

        // When
        assertThatThrownBy(() -> runTask())
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");

        // Then
        assertThat(getMessagesFromDlq()).isEmpty();
        assertThat(getMessagesFromQueue()).containsExactly(messageString);
    }

    @Test
    @Disabled("TODO")
    // This fails because the message isn't sent immediately to the dead letter queue, instead it's left to time out
    // with the standard queue visibility timeout.
    // With this specific failure we'd probably want it to go to the dead letter queue, since the partition does not
    // exist.
    // At the same time it shouldn't really be necessary to check the actual partition here, since the region is
    // included in the message. It looks like it is loading the partition because of how the compaction code is set up.
    // Maybe that should be changed?
    // It would be good if we could simulate an exception where we would actually want it to be retried after a delay,
    // e.g. some sort of networking failure. Maybe we should take more control in tests over how that would be handled.
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

        // Then
        assertThat(getMessagesFromQueue()).isEmpty();
        assertThat(getJobsFromDlq()).containsExactly(query);
    }

    @Test
    @Disabled("TODO")
    // This fails because the message isn't returned to the queue immediately, instead it's left to time out
    // with the standard queue visibility timeout.
    // From the test name I'm not sure what's intended here. I can't see why you'd want to retry an unreadable message.
    public void shouldMoveMessageToDlqAfterTwoFailures() throws Exception {
        // Given
        String messageString = "This will cause a failure!";
        configureJobQueuesWithMaxReceiveCount(2);
        send(messageString);

        // When
        // The task needs to be run twice for it to be moved to the DLQ
        assertThatThrownBy(() -> runTask())
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");
        assertThatThrownBy(() -> runTask())
                .hasMessageContaining("Expected BEGIN_OBJECT but was STRING");

        // Then
        assertThat(getMessagesFromQueue()).isEmpty();
        assertThat(getMessagesFromDlq()).containsExactly(messageString);
    }

    @Test
    public void shouldProcessNoMessages() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);

        // When
        runTask();

        // Then
        assertThat(getMessagesFromQueue()).isEmpty();
        assertThat(getMessagesFromDlq()).isEmpty();
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
        return streamMessagesFromQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL)).toList();
    }

    private List<String> getMessagesFromQueue() {
        return streamMessagesFromQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL)).toList();
    }

    private List<BulkExportLeafPartitionQuery> getJobsFromQueue() {
        return getJobsFromQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL));
    }

    private List<BulkExportLeafPartitionQuery> getJobsFromDlq() {
        return getJobsFromQueue(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL));
    }

    private List<BulkExportLeafPartitionQuery> getJobsFromQueue(String url) {
        BulkExportLeafPartitionQuerySerDe serDe = new BulkExportLeafPartitionQuerySerDe(
                new FixedTablePropertiesProvider(tableProperties));
        return streamMessagesFromQueue(url)
                .map(serDe::fromJson)
                .toList();
    }

    private Stream<String> streamMessagesFromQueue(String url) {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(url)
                .waitTimeSeconds(0)
                .maxNumberOfMessages(10)
                .build();
        return sqsClient.receiveMessage(request)
                .messages().stream().map(Message::body);
    }

    private void configureJobQueuesWithMaxReceiveCount(int maxReceiveCount) {
        sqsClient.setQueueAttributes(SetQueueAttributesRequest.builder()
                .queueUrl(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_URL))
                .attributes(Map.of(
                        QueueAttributeName.REDRIVE_POLICY, "{\"maxReceiveCount\":\"" + maxReceiveCount + "\", \"deadLetterTargetArn\":\"" + getJobDlqArn() + "\"}",
                        QueueAttributeName.VISIBILITY_TIMEOUT, instanceProperties.get(BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS)))
                .build());
    }

    private String getJobDlqArn() {
        return sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(instanceProperties.get(LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL))
                .attributeNames(List.of(QueueAttributeName.QUEUE_ARN))
                .build())
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);
    }
}
