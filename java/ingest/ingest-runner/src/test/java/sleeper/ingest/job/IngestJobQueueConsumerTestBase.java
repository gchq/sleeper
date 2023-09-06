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

package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.sqs.AmazonSQS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStoreException;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class IngestJobQueueConsumerTestBase {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.SQS,
            LocalStackContainer.Service.DYNAMODB,
            LocalStackContainer.Service.CLOUDWATCH);

    protected final AmazonS3 s3 = AWS_EXTERNAL_RESOURCE.getS3Client();
    protected final S3AsyncClient s3Async = AWS_EXTERNAL_RESOURCE.getS3AsyncClient();
    protected final AmazonSQS sqs = AWS_EXTERNAL_RESOURCE.getSqsClient();
    protected final AmazonDynamoDB dynamoDB = AWS_EXTERNAL_RESOURCE.getDynamoDBClient();
    protected final AmazonCloudWatch cloudWatch = AWS_EXTERNAL_RESOURCE.getCloudWatchClient();
    protected final Configuration hadoopConfiguration = AWS_EXTERNAL_RESOURCE.getHadoopConfiguration();

    private final String instanceId = UUID.randomUUID().toString();
    protected final String tableName = UUID.randomUUID().toString();
    private final String ingestQueueName = instanceId + "-ingestqueue";
    private final String configBucketName = instanceId + "-configbucket";
    private final String ingestDataBucketName = instanceId + "-" + tableName + "-ingestdata";
    private final String tableDataBucketName = instanceId + "-" + tableName + "-tabledata";
    private final String fileSystemPrefix = "s3a://";
    @TempDir
    public java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void before() throws IOException {
        s3.createBucket(configBucketName);
        s3.createBucket(tableDataBucketName);
        s3.createBucket(ingestDataBucketName);
        sqs.createQueue(ingestQueueName);
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    protected InstanceProperties getInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(CONFIG_BUCKET, configBucketName);
        instanceProperties.set(INGEST_JOB_QUEUE_URL, sqs.getQueueUrl(ingestQueueName).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }

    protected TableProperties createTable(Schema schema) throws IOException, StateStoreException {
        InstanceProperties instanceProperties = getInstanceProperties();

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, getTableDataBucket());
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        tableProperties.saveToS3(s3);

        new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDB).create();

        return tableProperties;
    }

    protected String getTableDataBucket() {
        return tableDataBucketName;
    }

    protected String getIngestBucket() {
        return ingestDataBucketName;
    }

    protected List<String> writeParquetFilesForIngest(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet", getIngestBucket(), subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path(fileSystemPrefix + fileWithoutSystemPrefix);
            ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(path, recordListAndSchema.sleeperSchema, hadoopConfiguration);
            for (Record record : recordListAndSchema.recordList) {
                writer.write(record);
            }
            writer.close();
        }

        return files;
    }
}
