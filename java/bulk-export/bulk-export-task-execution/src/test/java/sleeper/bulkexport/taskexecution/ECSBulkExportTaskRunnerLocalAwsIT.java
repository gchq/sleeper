/*
 * Copyright 2022-2026 Crown Copyright
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

import io.floci.testcontainers.FlociContainer;
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
import sleeper.bulkexport.taskexecution.ECSBulkExportTaskRunner.DataFusionQueryExporter;
import sleeper.bulkexport.taskexecution.ECSBulkExportTaskRunner.JavaCompactionExporter;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.tracker.job.run.RowsProcessed;
import sleeper.foreign.datafusion.DataFusionAwsConfig;
import sleeper.local.aws.test.LocalAwsTestBase;
import sleeper.local.aws.test.SleeperLocalAwsContainer;
import sleeper.parquet.row.ParquetReaderIterator;
import sleeper.parquet.row.ParquetRowReaderFactory;
import sleeper.parquet.row.ParquetRowWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_DELAY_BEFORE_RETRY_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_MAX_ALIVE_JITTER_IN_MINUTES;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_MAX_ALIVE_TIME_IN_MINUTES;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_MAX_CONSECUTIVE_FAILURES;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_MAX_IDLE_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.BulkExportProperty.BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_EXPORT_S3_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_DLQ_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.LEAF_PARTITION_BULK_EXPORT_QUEUE_URL;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public class ECSBulkExportTaskRunnerLocalAwsIT extends LocalAwsTestBase {
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
        tableProperties.setEnum(DATA_ENGINE, DataEngine.JAVA);
        instanceProperties.setNumber(BULK_EXPORT_TASK_WAIT_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(BULK_EXPORT_QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS, 1);
        instanceProperties.setNumber(BULK_EXPORT_JOB_FAILED_VISIBILITY_TIMEOUT_IN_SECONDS, 0);
        instanceProperties.setNumber(BULK_EXPORT_TASK_MAX_IDLE_TIME_IN_SECONDS, 0);
        instanceProperties.setNumber(BULK_EXPORT_TASK_DELAY_BEFORE_RETRY_IN_SECONDS, 0);
        instanceProperties.set(BULK_EXPORT_S3_BUCKET, UUID.randomUUID().toString());
        createBucket(instanceProperties.get(BULK_EXPORT_S3_BUCKET));
        createBucket(instanceProperties.get(DATA_BUCKET));
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClient).create();
    }

    @Test
    public void shouldRunAllBulkExportSubQueriesOnQueue() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Row row2 = new Row(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1, row2));
        BulkExportLeafPartitionQuery query1 = createQueryWithIdsAndFiles("e-1", "se-1", file).build();
        BulkExportLeafPartitionQuery query2 = createQueryWithIdsAndFiles("e-2", "se-2", file).build();
        send(query1);
        send(query2);

        // When
        runTask();

        // Then
        assertThat(readOutputFile(query1)).containsExactly(row1, row2);
        assertThat(readOutputFile(query2)).containsExactly(row1, row2);
        assertThat(getMessagesFromDlq()).isEmpty();
        assertThat(getJobsFromQueue()).isEmpty();
    }

    @Test
    public void shouldSendJobToDlqWhenSqlQueryGivenToJavaExporter() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Row row2 = new Row(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1, row2));
        BulkExportLeafPartitionQuery query1 = createQueryWithIdsAndFiles("e-1", "se-1", file)
                .sqlQuery("SELECT * FROM query_results;")
                .build();
        send(query1);

        // When
        runTask();

        // Then
        assertThat(getMessagesFromQueue()).isEmpty();
        assertThat(getJobsFromDlq()).containsExactly(query1);
    }

    @Test
    public void shouldContinueProcessingAfterFailedJob() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Row row2 = new Row(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1, row2));
        BulkExportLeafPartitionQuery failingQuery = createQueryWithIdsAndFiles("e-1", "se-1", file)
                .sqlQuery("SELECT * FROM query_results;")
                .build();
        BulkExportLeafPartitionQuery goodQuery = createQueryWithIdsAndFiles("e-2", "se-2", file).build();
        send(failingQuery);
        send(goodQuery);

        // When
        runTask();

        // Then
        assertThat(readOutputFile(goodQuery)).containsExactly(row1, row2);
        assertThat(getMessagesFromQueue()).isEmpty();
        assertThat(getJobsFromDlq()).containsExactly(failingQuery);
    }

    @Test
    public void shouldTerminateAfterMaxConsecutiveFailures() throws Exception {
        // Given the failing job can be received many times before it is moved to the DLQ,
        // and the task is set to terminate after 2 consecutive failures
        configureJobQueuesWithMaxReceiveCount(10);
        instanceProperties.setNumber(BULK_EXPORT_TASK_MAX_CONSECUTIVE_FAILURES, 2);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1));
        BulkExportLeafPartitionQuery failingQuery = createQueryWithIdsAndFiles("e-1", "se-1", file)
                .sqlQuery("SELECT * FROM query_results;")
                .build();
        send(failingQuery);

        // When
        runTask();

        // Then the job is still on the queue, having failed twice
        assertThat(getMessagesFromDlq()).isEmpty();
        assertThat(getJobsFromQueue()).containsExactly(failingQuery);
    }

    @Test
    public void shouldTerminateWhenMaxAliveTimeReached() throws Exception {
        // Given the task has been alive longer than the max alive time at the first check
        instanceProperties.setNumber(BULK_EXPORT_TASK_MAX_ALIVE_TIME_IN_MINUTES, 10);
        instanceProperties.setNumber(BULK_EXPORT_TASK_MAX_ALIVE_JITTER_IN_MINUTES, 0);
        configureJobQueuesWithMaxReceiveCount(1);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1));
        BulkExportLeafPartitionQuery query = createQueryWithIdsAndFiles("e-1", "se-1", file).build();
        send(query);
        Iterator<Instant> times = List.of(
                Instant.parse("2026-01-01T00:00:00Z"), // start time
                Instant.parse("2026-01-01T00:11:00Z")) // max alive time check
                .iterator();

        // When
        DataFusionAwsConfig awsConfig = createAwsConfig();
        new ECSBulkExportTaskRunner(
                new JavaCompactionExporter(instanceProperties, s3Client, hadoopConf, awsConfig),
                new DataFusionQueryExporter(awsConfig),
                times::next, Thread::sleep, () -> 0.0)
                .runECSBulkExportTaskRunner(instanceProperties, new FixedTablePropertiesProvider(tableProperties), sqsClient);

        // Then no message was processed as the task terminated immediately
        assertThat(getJobsFromQueue()).containsExactly(query);
        assertThat(getMessagesFromDlq()).isEmpty();
    }

    @Test
    public void shouldRunOneBulkExportSubQueryViaDataFusion() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION_EXPERIMENTAL);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Row row2 = new Row(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1, row2));
        BulkExportLeafPartitionQuery query1 = createQueryWithIdsAndFiles("e-1", "se-1", file).build();
        BulkExportLeafPartitionQuery query2 = createQueryWithIdsAndFiles("e-2", "se-2", file).build();
        send(query1);
        send(query2);
        ECSBulkExportTaskRunner.BulkExporter fakeJavaExporter = (query, outputFile, tableProps) -> {
            throw new IllegalStateException("Java exporter should not be called for DataFusion engine");
        };
        ECSBulkExportTaskRunner.BulkExporter fakeDataFusionExporter = (query, outputFile, tableProps) -> {
            writeRowsToPath(new Path(outputFile), List.of(row1, row2));
            return new RowsProcessed(2, 2);
        };

        // When
        runTask(fakeJavaExporter, fakeDataFusionExporter);

        // Then
        assertThat(readOutputFile(query1)).containsExactly(row1, row2);
        assertThat(readOutputFile(query2)).containsExactly(row1, row2);
        assertThat(getMessagesFromDlq()).isEmpty();
        assertThat(getJobsFromQueue()).isEmpty();
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
        runTask();

        // Then
        assertThat(getMessagesFromDlq()).isEmpty();
        assertThat(getMessagesFromQueue()).containsExactly(messageString);
    }

    @Test
    public void shouldHandleExceptionInProcessingAndSendToDlq() throws Exception {
        // Given
        configureJobQueuesWithMaxReceiveCount(1);
        Row row1 = new Row(Map.of("key", 5, "value1", "5", "value2", "some value"));
        Row row2 = new Row(Map.of("key", 15, "value1", "15", "value2", "other value"));
        FileReference file = addPartitionFile("L", "file", List.of(row1, row2));
        BulkExportLeafPartitionQuery query = createQueryWithIdsAndFiles("e-id", "se-id", file).build();
        send(query);
        ECSBulkExportTaskRunner.BulkExporter failingExporter = (job, outputFile, tableProps) -> {
            throw new RuntimeException("Export failed");
        };

        // When
        runTask(failingExporter, failingExporter);

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
        runTask();
        runTask();

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

    private BulkExportLeafPartitionQuery.Builder createQueryWithIdsAndFiles(
            String exportId, String subExportId, FileReference... files) {
        String partitionId = files[0].getPartitionId();
        return BulkExportLeafPartitionQuery.builder()
                .tableId(tableProperties.get(TABLE_ID))
                .exportId(exportId)
                .subExportId(subExportId)
                // Query across full region
                .regions(List.of(partitions.getPartition(partitionId).getRegion()))
                .leafPartitionId(partitionId)
                .partitionRegion(partitions.getPartition(partitionId).getRegion())
                .files(Stream.of(files).map(FileReference::getFilename).toList());
    }

    private void runTask() throws Exception {
        DataFusionAwsConfig awsConfig = createAwsConfig();
        runTask(new JavaCompactionExporter(instanceProperties, s3Client, hadoopConf, awsConfig),
                new DataFusionQueryExporter(awsConfig));
    }

    private void runTask(ECSBulkExportTaskRunner.BulkExporter javaExporter,
            ECSBulkExportTaskRunner.BulkExporter dataFusionExporter) throws Exception {
        new ECSBulkExportTaskRunner(javaExporter, dataFusionExporter)
                .runECSBulkExportTaskRunner(instanceProperties, new FixedTablePropertiesProvider(tableProperties), sqsClient);
    }

    private StateStore stateStore() {
        return new StateStoreFactory(instanceProperties, s3Client, dynamoClient).getStateStore(tableProperties);
    }

    private FileReference addPartitionFile(String partitionId, String name, List<Row> rows) {
        FileReference reference = fileFactory().partitionFile(partitionId, name, rows.size());
        writeRowsToPath(new Path(reference.getFilename()), rows);
        update(stateStore()).addFile(reference);
        return reference;
    }

    private void writeRowsToPath(Path path, Iterable<Row> rows) {
        try (ParquetWriter<Row> writer = ParquetRowWriterFactory.createParquetRowWriter(path, tableProperties, hadoopConf)) {
            for (Row row : rows) {
                writer.write(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Row> readOutputFile(BulkExportLeafPartitionQuery query) {
        Path path = new Path(query.getOutputFile(instanceProperties));
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetRowReaderFactory.parquetRowReaderBuilder(path, schema).withConf(hadoopConf).build())) {
            List<Row> rows = new ArrayList<>();
            reader.forEachRemaining(rows::add);
            return rows;
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

    private static DataFusionAwsConfig createAwsConfig() {
        FlociContainer container = SleeperLocalAwsContainer.INSTANCE;
        return DataFusionAwsConfig.overrideEndpoint(container.getEndpoint());
    }
}
