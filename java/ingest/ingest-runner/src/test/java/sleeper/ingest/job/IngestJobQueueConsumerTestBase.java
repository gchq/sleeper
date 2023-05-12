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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.testutils.AwsExternalResource;
import sleeper.ingest.testutils.RecordGenerator;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.FILE_IN_PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.FILE_LIFECYCLE_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public abstract class IngestJobQueueConsumerTestBase {
    @RegisterExtension
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.S3,
            LocalStackContainer.Service.SQS,
            LocalStackContainer.Service.DYNAMODB,
            LocalStackContainer.Service.CLOUDWATCH);
    protected static final String TEST_INSTANCE_NAME = "myinstance";
    protected static final String TEST_TABLE_NAME = "mytable";
    protected static final String INGEST_QUEUE_NAME = TEST_INSTANCE_NAME + "-ingestqueue";
    protected static final String CONFIG_BUCKET_NAME = TEST_INSTANCE_NAME + "-configbucket";
    protected static final String INGEST_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-ingestdata";
    protected static final String TABLE_DATA_BUCKET_NAME = TEST_INSTANCE_NAME + "-" + TEST_TABLE_NAME + "-tabledata";
    protected static final String FILE_SYSTEM_PREFIX = "s3a://";
    @TempDir
    public java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void before() throws IOException {
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(CONFIG_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(TABLE_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getS3Client().createBucket(INGEST_DATA_BUCKET_NAME);
        AWS_EXTERNAL_RESOURCE.getSqsClient().createQueue(INGEST_QUEUE_NAME);
    }

    @AfterEach
    public void after() {
        AWS_EXTERNAL_RESOURCE.clear();
    }

    protected InstanceProperties getInstanceProperties() {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, TEST_INSTANCE_NAME);
        instanceProperties.set(CONFIG_BUCKET, CONFIG_BUCKET_NAME);
        instanceProperties.set(INGEST_JOB_QUEUE_URL, AWS_EXTERNAL_RESOURCE.getSqsClient().getQueueUrl(INGEST_QUEUE_NAME).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, FILE_SYSTEM_PREFIX);
        instanceProperties.set(INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        return instanceProperties;
    }

    protected TableProperties createTable(Schema schema) throws IOException, StateStoreException {
        InstanceProperties instanceProperties = getInstanceProperties();

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, TEST_TABLE_NAME);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, getTableDataBucket());
        tableProperties.set(FILE_IN_PARTITION_TABLENAME, TEST_TABLE_NAME + "-fip");
        tableProperties.set(FILE_LIFECYCLE_TABLENAME, TEST_TABLE_NAME + "-fl");
        tableProperties.set(PARTITION_TABLENAME, TEST_TABLE_NAME + "-p");
        tableProperties.saveToS3(AWS_EXTERNAL_RESOURCE.getS3Client());

        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(
                instanceProperties,
                tableProperties,
                AWS_EXTERNAL_RESOURCE.getDynamoDBClient());
        dynamoDBStateStoreCreator.create();

        return tableProperties;
    }

    protected String getTableDataBucket() {
        return TABLE_DATA_BUCKET_NAME;
    }

    protected String getIngestBucket() {
        return INGEST_DATA_BUCKET_NAME;
    }

    protected List<String> writeParquetFilesForIngest(
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            String subDirectory,
            int numberOfFiles) throws IOException {
        List<String> files = new ArrayList<>();

        for (int fileNo = 0; fileNo < numberOfFiles; fileNo++) {
            String fileWithoutSystemPrefix = String.format("%s/%s/file-%d.parquet", getIngestBucket(), subDirectory, fileNo);
            files.add(fileWithoutSystemPrefix);
            Path path = new Path(FILE_SYSTEM_PREFIX + fileWithoutSystemPrefix);
            ParquetWriter<Record> writer =  ParquetRecordWriterFactory.createParquetRecordWriter(path, recordListAndSchema.sleeperSchema, AWS_EXTERNAL_RESOURCE.getHadoopConfiguration());
            for (Record record : recordListAndSchema.recordList) {
                writer.write(record);
            }
            writer.close();
        }

        return files;
    }
}
