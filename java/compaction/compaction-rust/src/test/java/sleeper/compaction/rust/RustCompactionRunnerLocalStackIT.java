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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.rust.RustCompactionRunner.AwsConfig;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableFilePaths;
import sleeper.core.tracker.job.run.RecordsProcessed;
import sleeper.localstack.test.SleeperLocalStackContainer;
import sleeper.parquet.record.ParquetReaderIterator;
import sleeper.parquet.record.ParquetRecordWriterFactory;
import sleeper.parquet.record.RecordReadSupport;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.sketches.testutils.SketchesDeciles;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreTestHelper.inMemoryStateStoreWithNoPartitions;
import static sleeper.ingest.runner.testutils.LocalStackAwsV2ClientHelper.buildAwsV2Client;
import static sleeper.parquet.utils.HadoopConfigurationLocalStackUtils.getHadoopConfiguration;

@Testcontainers
public class RustCompactionRunnerLocalStackIT {

    @Container
    private static final LocalStackContainer CONTAINER = SleeperLocalStackContainer.create(S3);

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
        String file1 = writeFileForPartition("root", List.of(record1));
        String file2 = writeFileForPartition("root", List.of(record2));
        CompactionJob job = createCompactionForPartition("test-job", "root", List.of(file1, file2));

        // When
        RecordsProcessed summary = compact(job);

        // Then
        assertThat(summary.getRecordsRead()).isEqualTo(2);
        assertThat(summary.getRecordsWritten()).isEqualTo(2);
        assertThat(readDataFile(schema, job.getOutputFile()))
                .containsExactly(record1, record2);
        assertThat(SketchesDeciles.from(readSketches(schema, job.getOutputFile())))
                .isEqualTo(SketchesDeciles.from(schema, List.of(record1, record2)));
    }

    protected CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected RecordsProcessed compact(CompactionJob job) throws Exception {
        CompactionRunner runner = new RustCompactionRunner(createAwsConfig());
        return runner.compact(job, tableProperties, stateStore.getPartition(job.getPartitionId()));
    }

    private static AwsConfig createAwsConfig() {
        return AwsConfig.builder()
                .region(CONTAINER.getRegion())
                .endpoint(CONTAINER.getEndpointOverride(S3).toString())
                .accessKey(CONTAINER.getAccessKey())
                .secretKey(CONTAINER.getSecretKey())
                .allowHttp(true)
                .build();
    }

    private String writeFileForPartition(String partitionId, List<Record> records) throws Exception {
        Schema schema = tableProperties.getSchema();
        Sketches sketches = Sketches.from(schema);
        Configuration configuration = getHadoopConfiguration(CONTAINER);
        String dataFile = buildPartitionFilePath(partitionId, UUID.randomUUID().toString() + ".parquet");
        try (ParquetWriter<Record> writer = ParquetRecordWriterFactory.createParquetRecordWriter(new org.apache.hadoop.fs.Path(dataFile), schema, configuration)) {
            for (Record record : records) {
                writer.write(record);
                sketches.update(record);
            }
        }
        org.apache.hadoop.fs.Path sketchesPath = SketchesSerDeToS3.sketchesPathForDataFile(dataFile);
        new SketchesSerDeToS3(schema).saveToHadoopFS(sketchesPath, sketches, configuration);
        return dataFile;
    }

    private CompactionJob createCompactionForPartition(String jobId, String partitionId, List<String> filenames) {
        return compactionFactory().createCompactionJobWithFilenames(jobId, filenames, partitionId);
    }

    private String buildPartitionFilePath(String partitionId, String filename) {
        return TableFilePaths.buildDataFilePathPrefix(instanceProperties, tableProperties)
                .constructPartitionParquetFilePath(partitionId, filename);
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

    private Sketches readSketches(Schema schema, String filename) throws IOException {
        org.apache.hadoop.fs.Path sketchesPath = SketchesSerDeToS3.sketchesPathForDataFile(filename);
        return new SketchesSerDeToS3(schema).loadFromHadoopFS(sketchesPath, getHadoopConfiguration(CONTAINER));
    }
}
