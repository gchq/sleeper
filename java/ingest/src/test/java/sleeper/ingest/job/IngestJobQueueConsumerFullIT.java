/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest.job;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.ingest.testutils.ResultVerifier;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class IngestJobQueueConsumerFullIT {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.SQS,
            LocalStackContainer.Service.DYNAMODB,
            LocalStackContainer.Service.CLOUDWATCH);
    private static final String TEST_INSTANCE_NAME = "myinstance";
    private static final String TEST_TABLE_NAME = "mytable";
    private static final String INGEST_QUEUE_NAME = TEST_INSTANCE_NAME + "-ingestqueue";
    private static final String CONFIG_BUCKET_NAME = TEST_INSTANCE_NAME + "-configbucket";
    private static final String INGEST_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-ingestdata";
    private static final String TABLE_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-tabledata";
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    public static final String FILE_SYSTEM_PREFIX = "s3a://";

    @Before
    public void before() throws IOException {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(CONFIG_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(TABLE_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(INGEST_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getSqsClient().createQueue(INGEST_QUEUE_NAME);
    }

    @After
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    private InstanceProperties getInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, TEST_INSTANCE_NAME);
        instanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        instanceProperties.set(INGEST_JOB_QUEUE_URL, AWS_EXTERNAL_RESOURCE.getSqsClient().getQueueUrl(INGEST_QUEUE_NAME).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, FILE_SYSTEM_PREFIX);
        instanceProperties.set(INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }

    private TableProperties createTable(Schema schema) throws IOException, StateStoreException {
        InstanceProperties instanceProperties = getInstanceProperties();

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, TEST_TABLE_NAME);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, getTableDataBucket());
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, TEST_TABLE_NAME + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, TEST_TABLE_NAME + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, TEST_TABLE_NAME + "-p");
        tableProperties.saveToS3(AWS_EXTERNAL_RESOURCE.getS3Client());

        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(
                instanceProperties,
                tableProperties,
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        dynamoDBStateStoreCreator.create();

        return tableProperties;
    }

    private String getTableDataBucket() {
        return TABLE_DATA_BUCKET_NAME;
    }

    private String getIngestBucket() {
        return INGEST_DATA_BUCKET_NAME;
    }

    private List<String> writeParquetFilesForIngest(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet", getIngestBucket(), subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            ParquetWriter<Record> writer = new ParquetRecordWriter.Builder(new Path(FILE_SYSTEM_PREFIX + fileWithoutSystemPrefix),
                    SchemaConverter.getSchema(recordListAndSchema.sleeperSchema), recordListAndSchema.sleeperSchema)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withConf(AWS_EXTERNAL_RESOURCE.getHadoopConfiguration())
                    .build();
            for (Record record : recordListAndSchema.recordList) {
                writer.write(record);
            }
            writer.close();
        }

        return files;
    }

    private void consumeAndVerify(Schema sleeperSchema,
                                  List<Record> expectedRecordList,
                                  int expectedNoOfFiles) throws Exception {
        String localDir = temporaryFolder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = getInstanceProperties();
        TableProperties tableProperties = createTable(sleeperSchema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(AWS_EXTERNAL_RESOURCE.getS3Client(), instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(AWS_EXTERNAL_RESOURCE.getDynamoDBClient(), new InstanceProperties());
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();

        // Run the job consumer
        IngestJobQueueConsumer ingestJobQueueConsumer = new IngestJobQueueConsumer(
                new ObjectFactory(instanceProperties, null, temporaryFolder.newFolder().getAbsolutePath()),
                AWS_EXTERNAL_RESOURCE.getSqsClient(),
                AWS_EXTERNAL_RESOURCE.getCloudWatchClient(),
                instanceProperties,
                tablePropertiesProvider,
                stateStoreProvider,
                localDir,
                AWS_EXTERNAL_RESOURCE.getS3AsyncClient(),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
        ingestJobQueueConsumer.run();

        // Verify the results
        ResultVerifier.verify(
                stateStore,
                sleeperSchema,
                key -> 0,
                expectedRecordList,
                Collections.singletonMap(0, expectedNoOfFiles),
                AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                temporaryFolder.newFolder().getAbsolutePath());
    }

    @Test
    public void shouldIngestParquetFilesPutOnTheQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 2);
        List<Record> doubledRecords = Stream.of(recordListAndSchema.recordList, recordListAndSchema.recordList)
                .flatMap(List::stream)
                .sorted(new RecordComparator(recordListAndSchema.sleeperSchema))
                .collect(Collectors.toList());
        IngestJob ingestJob = new IngestJob(TEST_TABLE_NAME, "id", files);
        AWS_EXTERNAL_RESOURCE.getSqsClient()
                .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob));
        consumeAndVerify(recordListAndSchema.sleeperSchema, doubledRecords, 1);
    }

    @Test
    public void shouldBeAbleToHandleAllFileFormatsPutOnTheQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "", 1);
        URI uri1 = new URI(FILE_SYSTEM_PREFIX + getIngestBucket() + "/file-1.crc");
        FileSystem.get(uri1, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration()).createNewFile(new Path(uri1));
        files.add(getIngestBucket() + "/file-1.crc");
        URI uri2 = new URI(FILE_SYSTEM_PREFIX + getIngestBucket() + "/file-2.csv");
        FileSystem.get(uri2, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration()).createNewFile(new Path(uri2));
        files.add(getIngestBucket() + "/file-2.csv");
        IngestJob ingestJob = new IngestJob(TEST_TABLE_NAME, "id", files);
        AWS_EXTERNAL_RESOURCE.getSqsClient()
                .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob));
        consumeAndVerify(recordListAndSchema.sleeperSchema, recordListAndSchema.recordList, 1);
    }

    @Test
    public void shouldIngestParquetFilesInNestedDirectoriesPutOnTheQueue() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        int noOfTopLevelDirectories = 5;
        int noOfNestings = 4;
        int noOfFilesPerDirectory = 3;
        List<String> files = IntStream.range(0, noOfTopLevelDirectories)
                .mapToObj(topLevelDirNo ->
                        IntStream.range(0, noOfNestings).mapToObj(nestingNo -> {
                            try {
                                String dirName = String.format("dir-%d%s", topLevelDirNo, String.join("", Collections.nCopies(nestingNo, "/nested-dir")));
                                return writeParquetFilesForIngest(recordListAndSchema, dirName, noOfFilesPerDirectory);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }).flatMap(List::stream).collect(Collectors.toList()))
                .flatMap(List::stream).collect(Collectors.toList());
        List<Record> expectedRecords = Stream.of(Collections.nCopies(noOfTopLevelDirectories * noOfNestings * noOfFilesPerDirectory, recordListAndSchema.recordList))
                .flatMap(List::stream)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        IngestJob ingestJob = new IngestJob(TEST_TABLE_NAME, "id", files);
        AWS_EXTERNAL_RESOURCE.getSqsClient()
                .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob));
        consumeAndVerify(recordListAndSchema.sleeperSchema, expectedRecords, 1);
    }

    @Test
    public void shouldContinueReadingFromQueueWhileMoreMessagesExist() throws Exception {
        RecordGenerator.RecordListAndSchema recordListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-100, 100).boxed().collect(Collectors.toList()));
        int noOfJobs = 10;
        int noOfFilesPerJob = 4;
        List<IngestJob> ingestJobs = IntStream.range(0, noOfJobs)
                .mapToObj(jobNo -> {
                    try {
                        List<String> files = writeParquetFilesForIngest(recordListAndSchema, "job-" + jobNo, noOfFilesPerJob);
                        return new IngestJob(TEST_TABLE_NAME, UUID.randomUUID().toString(), files);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        List<Record> expectedRecords = Stream.of(Collections.nCopies(noOfJobs * noOfFilesPerJob, recordListAndSchema.recordList))
                .flatMap(List::stream)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        ingestJobs.forEach(ingestJob ->
                AWS_EXTERNAL_RESOURCE.getSqsClient()
                        .sendMessage(getInstanceProperties().get(INGEST_JOB_QUEUE_URL), new IngestJobSerDe().toJson(ingestJob)));
        consumeAndVerify(recordListAndSchema.sleeperSchema, expectedRecords, noOfJobs);
    }
}
