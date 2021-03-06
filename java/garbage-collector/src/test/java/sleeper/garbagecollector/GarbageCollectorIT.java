package sleeper.garbagecollector;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import sleeper.configuration.properties.InstanceProperties;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import static sleeper.configuration.properties.table.TableProperty.ACTIVE_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.READY_FOR_GC_FILEINFO_TABLENAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import sleeper.core.CommonTestConstants;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStoreCreator;
import sleeper.table.job.TableLister;
import sleeper.table.util.StateStoreProvider;

public class GarbageCollectorIT {

    @ClassRule
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(
            LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3
    );

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

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
        Schema schema = new Schema();
        schema.setRowKeyFields(new Field("key", new IntType()));
        schema.setValueFields(new Field("value", new StringType()));
        return schema;
    }

    @Test
    public void shouldGarbageCollect() throws StateStoreException, IOException, InterruptedException {
        // Given
        AmazonS3 s3Client = createS3Client();
        AmazonDynamoDB dynamoDBClient = createDynamoClient();
        Schema schema = getSchema();
        String tableName = UUID.randomUUID().toString();
        String localDir = folder.newFolder().getAbsolutePath();
        InstanceProperties instanceProperties = createInstanceProperties(s3Client);
        TableProperties tableProperties = createTable(s3Client, dynamoDBClient, instanceProperties, tableName, localDir, schema);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
        StateStore stateStore = stateStoreProvider.getStateStore(tableProperties);
        stateStore.initialise();
        System.out.println(tableProperties);
        TableLister tableLister = new TableLister(s3Client, instanceProperties);
        Partition partition = stateStore.getAllPartitions().get(0);
        String tempFolder = folder.newFolder().getAbsolutePath();
        //  - A file which should be garbage collected immediately
        String file1 = tempFolder + "/file1.parquet";
        FileInfo fileInfo1 = new FileInfo();
        fileInfo1.setRowKeyTypes(new IntType());
        fileInfo1.setFilename(file1);
        fileInfo1.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo1.setPartitionId(partition.getId());
        fileInfo1.setMinRowKey(Key.create(1));
        fileInfo1.setMaxRowKey(Key.create(100));
        fileInfo1.setNumberOfRecords(100L);
        fileInfo1.setLastStateStoreUpdateTime(System.currentTimeMillis() - 100000);
        ParquetRecordWriter writer1 = new ParquetRecordWriter(new Path(file1), SchemaConverter.getSchema(schema), schema);
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
        FileInfo fileInfo2 = new FileInfo();
        fileInfo2.setRowKeyTypes(new IntType());
        fileInfo2.setFilename(file2);
        fileInfo2.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo2.setPartitionId(partition.getId());
        fileInfo2.setMinRowKey(Key.create(1));
        fileInfo2.setMaxRowKey(Key.create(100));
        fileInfo2.setNumberOfRecords(100L);
        fileInfo2.setLastStateStoreUpdateTime(System.currentTimeMillis());
        ParquetRecordWriter writer2 = new ParquetRecordWriter(new Path(file2), SchemaConverter.getSchema(schema), schema);
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
        FileInfo fileInfo3 = new FileInfo();
        fileInfo3.setRowKeyTypes(new IntType());
        fileInfo3.setFilename(file3);
        fileInfo3.setFileStatus(FileInfo.FileStatus.READY_FOR_GARBAGE_COLLECTION);
        fileInfo3.setPartitionId(partition.getId());
        fileInfo3.setMinRowKey(Key.create(1));
        fileInfo3.setMaxRowKey(Key.create(100));
        fileInfo3.setNumberOfRecords(100L);
        fileInfo3.setLastStateStoreUpdateTime(System.currentTimeMillis());
        ParquetRecordWriter writer3 = new ParquetRecordWriter(new Path(file3), SchemaConverter.getSchema(schema), schema);
        for (int i = 0; i < 100; i++) {
            Record record = new Record();
            record.put("key", i);
            record.put("value", "" + i);
            writer3.write(record);
        }
        writer3.close();
        stateStore.addFile(fileInfo3);

        Configuration conf = new Configuration();
        GarbageCollector garbageCollector = new GarbageCollector(conf, tableLister, tablePropertiesProvider, stateStoreProvider,10);

        // When
        Thread.sleep(1000L);
        garbageCollector.run(); // This should remove file 1 but leave files 2 and 3

        // Then
        //  - There should be no more files currently ready for garbage collection
        assertFalse(stateStore.getReadyForGCFiles().hasNext());
        //  - File1 should have been deleted
        assertFalse(Files.exists(new File(file1).toPath()));
        //  - The active file should still be there
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertEquals(1, activeFiles.size());
        assertEquals(fileInfo2, activeFiles.get(0));
        //  - The ready for GC table should still have 1 item in (but it's not returned by getReadyForGCFiles()
        //      because it is less than 10 seconds since it was marked as ready for GC). As the StateStore API
        //      does not have a method to return all values in the ready for gc table, we query the table
        //      directly.
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableProperties.get(READY_FOR_GC_FILEINFO_TABLENAME))
                .withConsistentRead(true);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        assertEquals(1, scanResult.getItems().size());
        assertEquals(file3, scanResult.getItems().get(0).get(DynamoDBStateStore.FILE_NAME).getS());

        s3Client.shutdown();
        dynamoDBClient.shutdown();
    }
}
