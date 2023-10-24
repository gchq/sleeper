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
package sleeper.compaction.job;

import org.apache.commons.lang3.tuple.MutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.FixedTablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.configuration.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class CompactionJobSerDeTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    private CompactionJobSerDe compactionJobSerDe() {
        return new CompactionJobSerDe(new FixedTablePropertiesProvider(tableProperties));
    }

    @BeforeEach
    void setUp() {
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
    }

    private CompactionJob.Builder jobForTable() {
        return CompactionJob.builder()
                .tableName(tableProperties.get(TABLE_NAME))
                .tableId(tableProperties.get(TABLE_ID));
    }

    private Schema schemaWithStringKey() {
        return Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    }

    private Schema schemaWith2StringKeysAndOneOfType(PrimitiveType type) {
        return Schema.builder()
                .rowKeyFields(
                        new Field("key1", new StringType()),
                        new Field("key2", new StringType()),
                        new Field("key3", type))
                .build();
    }

    @Test
    public void shouldSerDeCorrectlyForNonSplittingJobWithNoIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable()
                .jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("partition1")
                .isSplittingJob(false).build();
        tableProperties.setSchema(schemaWithStringKey());
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForNonSplittingJobWithIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable()
                .jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("partition1")
                .isSplittingJob(false)
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1")
                .build();
        tableProperties.setSchema(schemaWithStringKey());
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobStringKeyWithNoIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable()
                .jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"))
                .partitionId("partition1")
                .isSplittingJob(true)
                .splitPoint("G")
                .dimension(2)
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .build();
        tableProperties.setSchema(schemaWith2StringKeysAndOneOfType(new StringType()));
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobIntKeyWithIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable().jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"))
                .partitionId("partition1")
                .isSplittingJob(true)
                .splitPoint(10)
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1")
                .dimension(2)
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .build();
        tableProperties.setSchema(schemaWith2StringKeysAndOneOfType(new IntType()));
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobLongKeyWithIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable().jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"))
                .partitionId("partition1")
                .isSplittingJob(true)
                .splitPoint(10L)
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1")
                .dimension(2)
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .build();
        tableProperties.setSchema(schemaWith2StringKeysAndOneOfType(new LongType()));
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobStringKeyWithIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable().jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"))
                .partitionId("partition1")
                .isSplittingJob(true)
                .splitPoint("G")
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1")
                .dimension(2)
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .build();
        tableProperties.setSchema(schemaWith2StringKeysAndOneOfType(new StringType()));
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForSplittingJobByteArrayKeyWithIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable()
                .jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFiles(new MutablePair<>("leftoutputfile", "rightoutputfile"))
                .partitionId("partition1")
                .isSplittingJob(true)
                .splitPoint(new byte[]{1, 2, 4, 8})
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1")
                .dimension(2)
                .childPartitions(Arrays.asList("childPartition1", "childPartition2"))
                .build();
        tableProperties.setSchema(schemaWith2StringKeysAndOneOfType(new ByteArrayType()));
        CompactionJobSerDe compactionJobSerDe = compactionJobSerDe();

        // When
        CompactionJob deserialisedCompactionJob = compactionJobSerDe.deserialiseFromString(compactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }
}
