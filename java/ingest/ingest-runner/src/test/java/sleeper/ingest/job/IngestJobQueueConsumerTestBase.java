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

package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreFactory;
import sleeper.statestore.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_QUEUE_WAIT_TIME;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public abstract class IngestJobQueueConsumerTestBase {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB,
                    LocalStackContainer.Service.SQS, LocalStackContainer.Service.CLOUDWATCH);

    protected final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    protected final S3AsyncClient s3Async = buildAwsV2Client(localStackContainer, LocalStackContainer.Service.S3, S3AsyncClient.builder());
    protected final AmazonSQS sqs = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.SQS, AmazonSQSClientBuilder.standard());
    protected final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.DYNAMODB, AmazonDynamoDBClientBuilder.standard());
    protected final AmazonCloudWatch cloudWatch = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.CLOUDWATCH, AmazonCloudWatchClientBuilder.standard());
    protected final Configuration hadoopConfiguration = getHadoopConfiguration(localStackContainer);

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3, dynamoDB);
    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    protected final String instanceId = instanceProperties.get(ID);
    protected final String tableName = tableProperties.get(TABLE_NAME);
    private final String ingestQueueName = instanceId + "-ingestqueue";
    private final String configBucketName = instanceProperties.get(CONFIG_BUCKET);
    private final String ingestDataBucketName = instanceId + "-ingestdata";
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final String fileSystemPrefix = "s3a://";
    @TempDir
    public java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void before() {
        s3.createBucket(configBucketName);
        s3.createBucket(dataBucketName);
        s3.createBucket(ingestDataBucketName);
        sqs.createQueue(ingestQueueName);
        instanceProperties.set(INGEST_JOB_QUEUE_URL, sqs.getQueueUrl(ingestQueueName).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        instanceProperties.set(INGEST_JOB_QUEUE_WAIT_TIME, "0");
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoDB).create();
    }

    protected StateStore createTable(Schema schema) throws IOException, StateStoreException {
        tableProperties.setSchema(schema);
        tablePropertiesStore.save(tableProperties);
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3, dynamoDB, hadoopConfiguration)
                .getStateStore(tableProperties);
        stateStore.initialise();
        return stateStore;
    }

    protected List<String> writeParquetFilesForIngest(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet", ingestDataBucketName, subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path(fileSystemPrefix + fileWithoutSystemPrefix);
            try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(
                    path, recordListAndSchema.sleeperSchema, hadoopConfiguration)) {
                for (Record record : recordListAndSchema.recordList) {
                    writer.write(record);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return files;
    }
}
