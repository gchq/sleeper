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
package sleeper.compaction.rust;

import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionRunner;
import sleeper.compaction.rust.RustCompactionRunner.AwsConfig;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.CommonTestConstants;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.IngestFactory;
import sleeper.ingest.IngestResult;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.RecordReadSupport;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.AssignJobIdRequest.assignJobOnPartitionToFiles;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;
import static sleeper.ingest.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.io.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class RustCompactionRunnerLocalStackIT {

    @Container
    private static final LocalStackContainer CONTAINER = new LocalStackContainer(
            DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE)).withServices(S3);

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final StateStore stateStore = inMemoryStateStoreWithNoPartitions();
    @TempDir
    public Path tempDir;

    @BeforeEach
    void setUp() {
        try (S3Client s3 = buildAwsV2Client(CONTAINER, S3, S3Client.builder())) {
            s3.createBucket(builder -> builder.bucket(instanceProperties.get(DATA_BUCKET)));
        }
    }

    @Test
    void shouldRunCompactionJob() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new StringType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());
        Record record1 = new Record(Map.of("key", "record-1"));
        Record record2 = new Record(Map.of("key", "record-2"));
        FileReference file1 = ingestRecordsGetFile(List.of(record1));
        FileReference file2 = ingestRecordsGetFile(List.of(record2));
        CompactionJob job = compactionFactory().createCompactionJob("test-job", List.of(file1, file2), "root");
        stateStore.assignJobIds(List.of(assignJobOnPartitionToFiles("test-job", "root", List.of(file1.getFilename(), file2.getFilename()))));

        // When
        RecordsProcessed summary = compact(job);

        // Then
        assertThat(summary.getRecordsRead()).isEqualTo(2);
        assertThat(summary.getRecordsWritten()).isEqualTo(2);
        assertThat(readDataFile(schema, job.getOutputFile()))
                .containsExactly(record1, record2);
    }

    protected CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected RecordsProcessed compact(CompactionJob job) throws Exception {
        CompactionRunner runner = new RustCompactionRunner(
                new FixedTablePropertiesProvider(tableProperties),
                new FixedStateStoreProvider(tableProperties, stateStore),
                createAwsConfig());
        return runner.compact(job);
    }

    private static AwsConfig createAwsConfig() {
        return AwsConfig.builder()
                .region(CONTAINER.getRegion())
                .endpoint(CONTAINER.getEndpointOverride(S3).toString())
                .accessKey(CONTAINER.getAccessKey())
                .secretKey(CONTAINER.getSecretKey())
                .build();
    }

    protected FileReference ingestRecordsGetFile(List<Record> records) throws Exception {
        try (S3AsyncClient s3AsyncClient = buildAwsV2Client(CONTAINER, S3, S3AsyncClient.builder())) {
            IngestFactory factory = IngestFactory.builder()
                    .objectFactory(ObjectFactory.noUserJars())
                    .localDir(tempDir.toString())
                    .stateStoreProvider(new FixedStateStoreProvider(tableProperties, stateStore))
                    .instanceProperties(instanceProperties)
                    .hadoopConfiguration(getHadoopConfiguration(CONTAINER))
                    .s3AsyncClient(s3AsyncClient)
                    .build();
            IngestResult result = factory.ingestFromRecordIterator(tableProperties, records.iterator());
            List<FileReference> files = result.getFileReferenceList();
            if (files.size() != 1) {
                throw new IllegalStateException("Expected 1 file ingested, found: " + files);
            }
            return files.get(0);
        }
    }

    private List<Record> readDataFile(Schema schema, String filename) throws IOException {
        List<Record> results = new ArrayList<>();
        try (ParquetReaderIterator reader = new ParquetReaderIterator(
                ParquetReader.builder(new RecordReadSupport(schema), new org.apache.hadoop.fs.Path(filename))
                        .withConf(getHadoopConfiguration(CONTAINER)).build())) {
            while (reader.hasNext()) {
                results.add(new Record(reader.next()));
            }
        }
        return results;
    }
}
