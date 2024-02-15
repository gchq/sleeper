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

package sleeper.clients.status.update;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesStore;
import sleeper.configuration.table.index.DynamoDBTableIndexCreator;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableIdentity;
import sleeper.core.table.TableNotFoundException;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestRecords;
import sleeper.ingest.IngestResult;
import sleeper.statestore.StateStoreProvider;
import sleeper.statestore.s3.S3StateStore;
import sleeper.statestore.s3.S3StateStoreCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.table.TableIdentity.uniqueIdAndName;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class DeleteTableIT {
    @TempDir
    private Path tempDir;
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.DYNAMODB);

    private final AmazonS3 s3 = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private final AmazonDynamoDB dynamoDB = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonDynamoDBClientBuilder.standard());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key1");
    private final TablePropertiesStore propertiesStore = S3TableProperties.getStore(instanceProperties, s3, dynamoDB);
    private final Configuration conf = getHadoopConfiguration(localStackContainer);
    private final StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, conf);
    private String inputFolderName;

    @BeforeEach
    void setUp() throws IOException {
        instanceProperties.set(INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        s3.createBucket(instanceProperties.get(CONFIG_BUCKET));
        s3.createBucket(instanceProperties.get(DATA_BUCKET));
        DynamoDBTableIndexCreator.create(dynamoDB, instanceProperties);
        S3StateStoreCreator stateStoreCreator = new S3StateStoreCreator(instanceProperties, dynamoDB);
        stateStoreCreator.create();
        inputFolderName = createTempDirectory(tempDir, null).toString();
    }

    @Test
    void shouldDeleteTable() throws Exception {
        // Given
        TableProperties table = createTable(uniqueIdAndName("test-table-1", "table-1"));
        StateStore stateStoreBefore = createStateStore(table);
        stateStoreBefore.initialise(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 50L)
                .buildList());
        IngestResult result = ingestRecords(table, List.of(
                new Record(Map.of("key1", 25L)),
                new Record(Map.of("key1", 100L))));
        FileReference rootFile = result.getFileReferenceList().get(0);
        List<String> tableFilesInS3 = streamTableObjects(table)
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
        assertThat(tableFilesInS3.stream()
                .filter(key -> key.startsWith(tablePrefix(table, "statestore/files"))))
                .hasSize(2);
        assertThat(tableFilesInS3.stream()
                .filter(key -> key.startsWith(tablePrefix(table, "statestore/partitions"))))
                .hasSize(1);
        assertThat(tableFilesInS3.stream()
                .filter(key -> key.startsWith(tablePrefix(table, "partition_root")))
                .map(FilenameUtils::getName))
                .containsExactly(
                        FilenameUtils.getName(rootFile.getFilename()),
                        FilenameUtils.getName(rootFile.getFilename()).replace("parquet", "sketches"));

        // When
        deleteTable("table-1");

        // Then
        assertThatThrownBy(() -> propertiesStore.loadByName("table-1"))
                .isInstanceOf(TableNotFoundException.class);
        StateStore stateStoreAfter = stateStoreProvider.getStateStore(table);
        assertThat(stateStoreAfter.getAllPartitions()).isEmpty();
        assertThat(stateStoreAfter.getFileReferences()).isEmpty();
        assertThat(streamTableObjects(table)).isEmpty();
    }

    @Test
    void shouldFailToDeleteTableThatDoesNotExist() {
        // When / Then
        assertThatThrownBy(() -> deleteTable("table-1"))
                .isInstanceOf(TableNotFoundException.class);
    }

    private void deleteTable(String tableName) {
        new DeleteTable(instanceProperties, s3, propertiesStore, stateStoreProvider).delete(tableName);
    }

    private TableProperties createTable(TableIdentity tableIdentity) {
        TableProperties table = createTestTableProperties(instanceProperties, schema);
        table.set(TABLE_ID, tableIdentity.getTableUniqueId());
        table.set(TABLE_NAME, tableIdentity.getTableName());
        propertiesStore.save(table);
        return table;
    }

    private StateStore createStateStore(TableProperties tableProperties) {
        return new S3StateStore(instanceProperties, tableProperties, dynamoDB, conf);
    }

    private IngestResult ingestRecords(TableProperties tableProperties, List<Record> records) throws Exception {
        IngestFactory factory = IngestFactory.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .localDir(inputFolderName)
                .stateStoreProvider(stateStoreProvider)
                .instanceProperties(instanceProperties)
                .hadoopConfiguration(conf)
                .build();

        IngestRecords ingestRecords = factory.createIngestRecords(tableProperties);
        ingestRecords.init();
        for (Record record : records) {
            ingestRecords.write(record);
        }
        return ingestRecords.close();
    }

    private String tablePrefix(TableProperties tableProperties, String prefix) {
        return tableProperties.get(TABLE_ID) + "/" + prefix;
    }

    private Stream<S3ObjectSummary> streamTableObjects(TableProperties tableProperties) {
        return s3.listObjects(instanceProperties.get(DATA_BUCKET)).getObjectSummaries().stream()
                .filter(s3ObjectSummary -> s3ObjectSummary.getSize() > 0)
                .filter(s3ObjectSummary -> s3ObjectSummary.getKey().startsWith(
                        tableProperties.get(TABLE_ID)));
    }
}
