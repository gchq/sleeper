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

package sleeper.garbagecollector;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils;
import sleeper.statestore.FixedStateStoreProvider;
import sleeper.statestore.StateStoreProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.configuration.testutils.LocalStackAwsV1ClientHelper.buildAwsV1Client;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.ReplaceFileReferencesRequest.replaceJobFileReferences;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

@Testcontainers
public class GarbageCollectorS3IT {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);
    private static final String TEST_BUCKET = "test-bucket";
    private final AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());
    private TableProperties tableProperties;
    private StateStoreProvider stateStoreProvider;
    private static final Schema TEST_SCHEMA = getSchema();
    private static final String TEST_TABLE_NAME = "test-table";

    private final PartitionTree partitions = new PartitionsBuilder(TEST_SCHEMA).singlePartition("root").buildTree();
    private final FileReferenceFactory factory = FileReferenceFactory.from(partitions);
    private final List<TableProperties> tables = new ArrayList<>();
    private final Configuration configuration = HadoopConfigurationLocalStackUtils.getHadoopConfiguration(localStackContainer);

    @BeforeEach
    void setUp() {
        s3Client.createBucket(TEST_BUCKET);
    }

    StateStore setupStateStoreAndFixTime(Instant fixedTime) {
        StateStore stateStore = inMemoryStateStoreWithSinglePartition(TEST_SCHEMA);
        stateStore.fixFileUpdateTime(fixedTime);
        stateStoreProvider = new FixedStateStoreProvider(tableProperties, stateStore);
        return stateStore;
    }

    @Test
    void shouldContinueCollectingFilesIfTryingToDeleteFileThrowsIOException() throws Exception {
        // Given
        InstanceProperties instanceProperties = createInstanceProperties();
        tableProperties = createTableWithGCDelay(TEST_TABLE_NAME, instanceProperties, 10);
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        StateStore stateStore = setupStateStoreAndFixTime(oldEnoughTime);
        // Create a FileReference referencing a file in a bucket that does not exist
        FileReference oldFile1 = factory.rootFile("s3a://not-a-bucket/old-file-1.parquet", 100L);
        stateStore.addFile(oldFile1);
        // Perform a compaction on an existing file to create a readyForGC file
        s3Client.putObject("test-bucket", "old-file-2.parquet", "abc");
        s3Client.putObject("test-bucket", "new-file-2.parquet", "def");
        FileReference oldFile2 = factory.rootFile("s3a://" + TEST_BUCKET + "/old-file-2.parquet", 100L);
        FileReference newFile2 = factory.rootFile("s3a://" + TEST_BUCKET + "/new-file-2.parquet", 100L);
        stateStore.addFile(oldFile2);
        stateStore.assignJobIds(List.of(
                assignJobOnPartitionToFiles("job1", "root",
                        List.of(oldFile1.getFilename(), oldFile2.getFilename()))));
        stateStore.atomicallyReplaceFileReferencesWithNewOnes(List.of(replaceJobFileReferences(
                "job1", "root", List.of(oldFile1.getFilename(), oldFile2.getFilename()), newFile2)));

        // When
        GarbageCollector collector = createGarbageCollector(instanceProperties, stateStoreProvider);

        // And / Then
        assertThatThrownBy(() -> collector.runAtTime(currentTime, List.of(tableProperties)))
                .isInstanceOf(FailedGarbageCollectionException.class);
        assertThat(s3Client.doesObjectExist(TEST_BUCKET, "old-file-2.parquet")).isFalse();
        assertThat(s3Client.doesObjectExist(TEST_BUCKET, "new-file-2.parquet")).isTrue();
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeAndReadyForGCFilesReport(oldEnoughTime,
                        List.of(newFile2),
                        List.of(oldFile1.getFilename())));
    }

    private InstanceProperties createInstanceProperties() {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(FILE_SYSTEM, "s3a://");
        instanceProperties.set(DATA_BUCKET, TEST_BUCKET);
        return instanceProperties;
    }

    private TableProperties createTableWithGCDelay(String tableName, InstanceProperties instanceProperties, int gcDelay) {
        TableProperties tableProperties = createTestTableProperties(instanceProperties, TEST_SCHEMA);
        tableProperties.set(TABLE_NAME, tableName);
        tableProperties.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, gcDelay);
        tables.add(tableProperties);
        return tableProperties;
    }

    private GarbageCollector createGarbageCollector(InstanceProperties instanceProperties, StateStoreProvider stateStoreProvider) {
        return new GarbageCollector(configuration, instanceProperties, stateStoreProvider);
    }

    private static Schema getSchema() {
        return Schema.builder()
                .rowKeyFields(new Field("key", new IntType()))
                .valueFields(new Field("value", new StringType()))
                .build();
    }
}
