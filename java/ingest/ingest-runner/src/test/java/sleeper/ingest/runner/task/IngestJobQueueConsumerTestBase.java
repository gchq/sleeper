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

package sleeper.ingest.runner.task;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configurationv2.properties.S3TableProperties;
import sleeper.configurationv2.table.index.DynamoDBTableIndexCreator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.runner.testutils.RecordGenerator;
import sleeper.localstack.test.LocalStackTestBase;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.sketchesv2.store.S3SketchesStore;
import sleeper.sketchesv2.store.SketchesStore;
import sleeper.statestorev2.StateStoreFactory;
import sleeper.statestorev2.transactionlog.TransactionLogStateStoreCreator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.IngestProperty.INGEST_JOB_QUEUE_WAIT_TIME;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_FILES_COMMIT_ASYNC;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.instance.TableDefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

public abstract class IngestJobQueueConsumerTestBase extends LocalStackTestBase {

    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3ClientV2, dynamoClientV2);
    protected final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    protected final String instanceId = instanceProperties.get(ID);
    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final SketchesStore sketchesStore = new S3SketchesStore(s3ClientV2, s3TransferManager);
    private final String ingestQueueName = instanceId + "-ingestqueue";
    private final String configBucketName = instanceProperties.get(CONFIG_BUCKET);
    private final String ingestDataBucketName = instanceId + "-ingestdata";
    private final String dataBucketName = instanceProperties.get(DATA_BUCKET);
    private final String fileSystemPrefix = "s3a://";
    @TempDir
    public java.nio.file.Path temporaryFolder;

    @BeforeEach
    public void before() {
        createBucket(configBucketName);
        createBucket(dataBucketName);
        createBucket(ingestDataBucketName);
        sqsClient.createQueue(ingestQueueName);
        instanceProperties.set(INGEST_JOB_QUEUE_URL, sqsClient.getQueueUrl(ingestQueueName).getQueueUrl());
        instanceProperties.set(FILE_SYSTEM, fileSystemPrefix);
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        instanceProperties.set(DEFAULT_INGEST_FILES_COMMIT_ASYNC, "false");
        instanceProperties.set(INGEST_JOB_QUEUE_WAIT_TIME, "0");
        DynamoDBTableIndexCreator.create(dynamoClientV2, instanceProperties);
        new TransactionLogStateStoreCreator(instanceProperties, dynamoClientV2).create();
    }

    protected StateStore createTable(Schema schema) throws IOException {
        tableProperties.setSchema(schema);
        tablePropertiesStore.save(tableProperties);
        StateStore stateStore = new StateStoreFactory(instanceProperties, s3ClientV2, dynamoClientV2)
                .getStateStore(tableProperties);
        update(stateStore).initialise(schema);
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
                    path, recordListAndSchema.sleeperSchema, hadoopConf)) {
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
