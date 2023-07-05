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
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableLister;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.FILE_IN_PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.FILE_LIFECYCLE_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
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
        tableProperties.set(FILE_IN_PARTITION_TABLENAME, tableName + "-fip");
        tableProperties.set(FILE_LIFECYCLE_TABLENAME, tableName + "-fl");
        tableProperties.set(PARTITION_TABLENAME, tableName + "-p");
        tableProperties.set(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, "0.1");
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
        TableLister tableLister = new TableLister(s3Client, instanceProperties);
        Partition partition = stateStore.getAllPartitions().get(0);
        String tempFolder = createTempDirectory(folder, null).toString();
        //  - File1 should be garbage collected as we remove its file-in-partition record
        String file1 = tempFolder + "/file1.parquet";
        String file2 = tempFolder + "/file2.parquet";
        String file3 = tempFolder + "/file3.parquet";
        FileInfo fileInfo1 = writeFile(file1, partition, schema);
        FileInfo fileInfo2 = writeFile(file2, partition, schema);
        FileInfo fileInfo3 = writeFile(file3, partition, schema);
        stateStore.addFile(fileInfo1);
        stateStore.addFile(fileInfo2);
        stateStore.addFile(fileInfo3);
        GarbageCollector garbageCollector = new GarbageCollector(new Configuration(), tableLister, tablePropertiesProvider, stateStoreProvider, 10);

        // When 1
        //  - Run garbage collector - nothing should change
        garbageCollector.run();

        // Then 1
        //  - There should be no files ready for garbage collection
        assertThat(stateStore.getReadyForGCFiles().hasNext()).isFalse();
        //  - All files should still be there
        assertThat(Files.exists(new File(file1).toPath())).isTrue();
        assertThat(Files.exists(new File(file2).toPath())).isTrue();
        assertThat(Files.exists(new File(file3).toPath())).isTrue();
        assertThat(stateStore.getActiveFileList())
            .extracting(FileInfo::getFilename)
            .containsExactlyInAnyOrder(file1, file2, file3);

        // When 2
        //  - Replace file1 with file4 and immediately run garbage collector
        String file4 = tempFolder + "/file4.parquet";
        FileInfo fileInfo4 = writeFile(file4, partition, schema);
        List<FileInfo> fileInfo1List = new ArrayList<>();
        fileInfo1List.add(fileInfo1);
        stateStore.atomicallyRemoveFileInPartitionRecordsAndCreateNewActiveFile(fileInfo1List, fileInfo4);
        garbageCollector.run();

        // Then 2
        //  - Nothing should have changed yet
        //  - There should be no files ready for garbage collection
        assertThat(stateStore.getReadyForGCFiles().hasNext()).isFalse();
        //  - All files should still be there
        assertThat(Files.exists(new File(file1).toPath())).isTrue();
        assertThat(Files.exists(new File(file2).toPath())).isTrue();
        assertThat(Files.exists(new File(file3).toPath())).isTrue();
        assertThat(Files.exists(new File(file4).toPath())).isTrue();
        assertThat(stateStore.getActiveFileList())
            .extracting(FileInfo::getFilename)
            .containsExactlyInAnyOrder(file2, file3, file4);

        // When 3
        //  - Sleep for 7 seconds
        Thread.sleep(7000L);
        garbageCollector.run();
        garbageCollector.run();

        // Then 3
        //  - File1 should have been deleted, the rest should not have been deleted
        assertThat(Files.exists(new File(file1).toPath())).isFalse();
        assertThat(Files.exists(new File(file2).toPath())).isTrue();
        assertThat(Files.exists(new File(file3).toPath())).isTrue();
        assertThat(Files.exists(new File(file4).toPath())).isTrue();
        assertThat(stateStore.getActiveFileList())
            .extracting(FileInfo::getFilename)
            .containsExactlyInAnyOrder(file2, file3, file4);

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }

    private FileInfo writeFile(String filename, Partition partition, Schema schema) throws IOException {
        FileInfo fileInfo = FileInfo.builder()
                .rowKeyTypes(new IntType())
                .filename(filename)
                .fileStatus(FileInfo.FileStatus.FILE_IN_PARTITION)
                .partitionId(partition.getId())
                .minRowKey(Key.create(1))
                .maxRowKey(Key.create(100))
                .numberOfRecords(100L)
                .build();
        ParquetWriter<Record> writer1 = ParquetRecordWriterFactory.createParquetRecordWriter(new Path(filename), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer1.write(record);
        }
        writer1.close();
        return fileInfo;
    }
}
