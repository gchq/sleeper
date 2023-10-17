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
package sleeper.compaction.jobexecution;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobSerDe;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.PropertiesReloader;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TablePropertiesStore;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class CompactSortedFilesRunnerLocalStackIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    private final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    private final InstanceProperties instanceProperties = createInstance();
    private final StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, null);
    private final TablePropertiesStore tablePropertiesStore = new S3TablePropertiesStore(instanceProperties, s3, dynamoDB);
    private final TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(instanceProperties, s3, dynamoDB);
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTable();
    private final String tableName = tableProperties.get(TABLE_NAME);

    private InstanceProperties createInstance() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(COMPACTION_JOB_QUEUE_URL, sqs.createQueue(UUID.randomUUID().toString()).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, "");

        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        instanceProperties.saveToS3(s3);
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new DynamoDBStateStoreCreator(instanceProperties, dynamoDB).create();

        return instanceProperties;
    }

    private Schema createSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new LongType()))
                .valueFields(new Field("value1", new LongType()), new Field("value2", new LongType()))
                .build();
    }

    private TableProperties createTable() {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "5");
        tablePropertiesStore.save(tableProperties);
        try {
            stateStoreProvider.getStateStore(tableProperties).initialise();
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
        return tableProperties;
    }

    private StateStore stateStore() {
        return stateStoreProvider.getStateStore(tableProperties);
    }

    @AfterEach
    void tearDown() {
        s3.shutdown();
        dynamoDB.shutdown();
        sqs.shutdown();
    }

    @TempDir
    public java.nio.file.Path folder;

    @Test
    void shouldDeleteMessages() throws Exception {
        // Given
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDB);
        CompactionJobStatusStore jobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDB);
        CompactionTaskStatusStore taskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDB, instanceProperties);
        // - Create four files of sorted data
        String folderName = createTempDirectory(folder, null).toString();
        String file1 = folderName + "/file1.parquet";
        String file2 = folderName + "/file2.parquet";
        String file3 = folderName + "/file3.parquet";
        String file4 = folderName + "/file4.parquet";
        FileInfo fileInfo1 = FileInfo.builder()
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(100L)
                .build();
        FileInfo fileInfo2 = FileInfo.builder()
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(100L)
                .build();
        FileInfo fileInfo3 = FileInfo.builder()
                .filename(file3)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(100L)
                .build();
        FileInfo fileInfo4 = FileInfo.builder()
                .filename(file4)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId("root")
                .numberOfRecords(100L)
                .build();
        ParquetWriter<Record> writer1 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file1), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("value1", (long) 2 * i);
            record.put("value2", 987654321L);
            writer1.write(record);
        }
        writer1.close();
        ParquetWriter<Record> writer2 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file2), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer2.write(record);
        }
        writer2.close();
        ParquetWriter<Record> writer3 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file3), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i);
            record.put("value1", (long) 2 * i);
            record.put("value2", 987654321L);
            writer3.write(record);
        }
        writer3.close();
        ParquetWriter<Record> writer4 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file4), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", (long) 2 * i + 1);
            record.put("value1", 1001L);
            record.put("value2", 123456789L);
            writer4.write(record);
        }
        writer4.close();
        // - Update Dynamo state store with details of files
        stateStore().addFiles(Arrays.asList(fileInfo1, fileInfo2, fileInfo3, fileInfo4));
        // - Create two compaction jobs and put on queue
        CompactionJob compactionJob1 = CompactionJob.builder()
                .tableName(tableName)
                .jobId("job1")
                .partitionId("root")
                .dimension(0)
                .inputFiles(Arrays.asList(file1, file2))
                .isSplittingJob(false)
                .outputFile(folderName + "/output1.parquet").build();
        CompactionJob compactionJob2 = CompactionJob.builder()
                .tableName(tableName)
                .jobId("job2")
                .partitionId("root")
                .dimension(0)
                .inputFiles(Arrays.asList(file3, file4))
                .isSplittingJob(false)
                .outputFile(folderName + "/output2.parquet").build();
        CompactionJobSerDe jobSerDe = new CompactionJobSerDe(tablePropertiesProvider);
        String job1Json = jobSerDe.serialiseToString(compactionJob1);
        String job2Json = jobSerDe.serialiseToString(compactionJob2);
        SendMessageRequest sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMessageBody(job1Json);
        sqs.sendMessage(sendMessageRequest);
        sendMessageRequest = new SendMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withMessageBody(job2Json);
        sqs.sendMessage(sendMessageRequest);

        // When
        CompactSortedFilesRunner runner = new CompactSortedFilesRunner(
                instanceProperties, ObjectFactory.noUserJars(),
                tablePropertiesProvider, PropertiesReloader.neverReload(), stateStoreProvider, jobStatusStore, taskStatusStore,
                "task-id", instanceProperties.get(COMPACTION_JOB_QUEUE_URL), sqs, null, CompactionTaskType.COMPACTION,
                1, 0);
        runner.run();

        // Then
        // - There should be no messages left on the queue
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(instanceProperties.get(COMPACTION_JOB_QUEUE_URL))
                .withWaitTimeSeconds(2);
        ReceiveMessageResult result = sqs.receiveMessage(receiveMessageRequest);
        assertThat(result.getMessages()).isEmpty();
        // - Check DynamoDBStateStore has correct active files
        List<FileInfo> activeFiles = stateStore().getActiveFiles();
        assertThat(activeFiles)
                .extracting(FileInfo::getFilename)
                .containsExactlyInAnyOrder(compactionJob1.getOutputFile(), compactionJob2.getOutputFile());
    }
}
