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
package sleeper.garbagecollector;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordWriterFactory;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableLister;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

@Testcontainers
public class GarbageCollectorIT {

    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3
    );

    @TempDir
    public java.nio.file.Path folder;

    private AmazonDynamoDB createDynamoClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
                .build();
    }

    private AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(localStackContainer.getDefaultCredentialsProvider())
                .withEndpointConfiguration(localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .build();
    }

    private InstanceProperties createInstanceProperties(AmazonS3 s3Client) {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.set(ID, UUID.randomUUID().toString());
        instanceProperties.set(CONFIG_BUCKET, UUID.randomUUID().toString());
        instanceProperties.set(FILE_SYSTEM, "");

        s3Client.createBucket(instanceProperties.get(CONFIG_BUCKET));

        return instanceProperties;
    }

    private TableProperties createTable(AmazonS3 s3,
                                        AmazonDynamoDB dynamoDB,
                                        InstanceProperties instanceProperties,
                                        String tableName,
                                        String dataBucket,
                                        Schema schema) throws IOException, StateStoreException {
        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setSchema(schema);
        tableProperties.set(DATA_BUCKET, dataBucket);
        tableProperties.set(ACTIVE_FILEINFO_TABLENAME, tableName + "-af");
        tableProperties.set(READY_FOR_GC_FILEINFO_TABLENAME, tableName + "-rfgcf");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "10");
        tableProperties.saveToS3(s3);

        DynamoDBStateStoreCreator dynamoDBStateStoreCreator = new DynamoDBStateStoreCreator(instanceProperties,
                tableProperties, dynamoDB);
        dynamoDBStateStoreCreator.create();
        return tableProperties;
    }

    private Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }

    @Test
    public void shouldGarbageCollect() throws StateStoreException, IOException, InterruptedException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        Schema schema = getSchema();
        String tableName = UUID.randomUUID().toString();
        String localDir = createTempDirectory(folder, null).toString();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        System.out.println(tableProperties);
        TableLister tableLister = new TableLister(s3Client, instanceProperties);
        Partition partition = stateStore.getAllPartitions().get(0);
        String tempFolder = createTempDirectory(folder, null).toString();
        //  - A file which should be garbage collected immediately
        String file1 = tempFolder + "/file1.parquet";
        FileInfo fileInfo1 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename(file1)
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(System.currentTimeMillis() - 20L * 60L * 1000L)
                .build();
        ParquetWriter<Record> writer1 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file1), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer1.write(record);
        }
        writer1.close();
        stateStore.addFile(fileInfo1);
        //  - An active file which should not be garbage collected
        String file2 = tempFolder + "/file2.parquet";
        FileInfo fileInfo2 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename(file2)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(System.currentTimeMillis())
                .build();
        ParquetWriter<Record> writer2 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file2), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer2.write(record);
        }
        writer2.close();
        stateStore.addFile(fileInfo2);
        //  - A file which is ready for garbage collection but which should not be garbage collected now as it has only
        //      just been marked as ready for GC
        String file3 = tempFolder + "/file3.parquet";
        FileInfo fileInfo3 = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename(file3)
                .fileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .lastStateStoreUpdateTime(System.currentTimeMillis())
                .build();
        ParquetWriter<Record> writer3 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(file3), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer3.write(record);
        }
        writer3.close();
        stateStore.addFile(fileInfo3);

        Configuration conf = new Configuration();
        GarbageCollector garbageCollector = new GarbageCollector(conf, tableLister, tablePropertiesProvider, stateStoreProvider, 10);

        // When
        Thread.sleep(1000L);
        garbageCollector.run(); // This should remove file 1 but leave files 2 and 3

        // Then
        //  - There should be no more files currently ready for garbage collection
        assertThat(stateStore.getReadyForGCFiles().hasNext()).isFalse();
        //  - File1 should have been deleted
        assertThat(Files.exists(new File(file1).toPath())).isFalse();
        //  - The active file should still be there
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles).containsExactly(fileInfo2);
        //  - The ready for GC table should still have 1 item in (but it's not returned by getReadyForGCFiles()
        //      because it is less than 10 seconds since it was marked as ready for GC). As the StateStore API
        //      does not have a method to return all values in the ready for gc table, we query the table
        //      directly.
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertThat(scanResult.getItems())
                .extracting(item -> item.get(DynamoDBStateStore.FILE_NAME).getS())
                .containsExactly(file3);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
