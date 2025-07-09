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
package sleeper.ingest.runner.impl;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.runner.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.runner.testutils.RecordGenerator;
import sleeper.ingest.runner.testutils.TestFilesAndRecords;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.sketches.testutils.SketchesDecilesComparator;

import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_BATCH_BUFFER_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_MAX_LOCAL_STORE_BYTES;
import static sleeper.core.properties.instance.ArrowIngestProperty.ARROW_INGEST_WORKING_BUFFER_BYTES;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;

class IngestCoordinatorUsingDirectWriteBackedByArrowIT extends DirectWriteBackedByArrowTestBase {
    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsInAPartitionThanCanFitInMemory() throws Exception {
        RecordGenerator.RowListAndSchema rowListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        update(stateStore).initialise(new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildList());
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        ingestRecords(rowListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 4 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 128 * 1024 * 1024L);
        });

        // Then
        TestFilesAndRecords actualActiveData = TestFilesAndRecords.loadActiveFiles(stateStore, rowListAndSchema.sleeperSchema, configuration);

        assertThat(actualActiveData.getFiles())
                .extracting(FileReference::getPartitionId, FileReference::getFilename)
                .containsExactlyInAnyOrder(
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile.parquet"));

        assertThat(actualActiveData.getSetOfAllRecords())
                .isEqualTo(new HashSet<>(rowListAndSchema.rowList));
        assertThat(actualActiveData.getPartitionData("left").streamAllRecords())
                .extracting(record -> record.get("key0"))
                .containsExactlyElementsOf(LongStream.range(-10000, 0).boxed()
                        .collect(Collectors.toList()));
        assertThat(actualActiveData.getPartitionData("right").streamAllRecords())
                .extracting(record -> record.get("key0"))
                .containsExactlyElementsOf(LongStream.range(0, 10000).boxed()
                        .collect(Collectors.toList()));
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualActiveData.getFiles(), new LocalFileSystemSketchesStore()))
                .usingComparator(SketchesDecilesComparator.longsMaxDiff(rowListAndSchema.sleeperSchema, 50))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @Test
    void shouldWriteRecordsWhenThereAreMoreRecordsThanCanFitInLocalFile() throws Exception {
        // Given
        RecordGenerator.RowListAndSchema rowListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        update(stateStore).initialise(new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildList());
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile1", "rightFile1", "leftFile2", "rightFile2"))
                .build();

        // When
        ingestRecords(rowListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 4 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 16 * 1024 * 1024L);
        });

        // Then
        TestFilesAndRecords actualActiveData = TestFilesAndRecords.loadActiveFiles(stateStore, rowListAndSchema.sleeperSchema, configuration);

        assertThat(actualActiveData.getFiles())
                .extracting(FileReference::getPartitionId, FileReference::getFilename)
                .containsExactlyInAnyOrder(
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile1.parquet"),
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile2.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile1.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile2.parquet"));
        assertThat(actualActiveData.getSetOfAllRecords())
                .isEqualTo(new HashSet<>(rowListAndSchema.rowList));
        assertThat(actualActiveData.getPartitionData("left"))
                .satisfies(data -> assertThat(data.getFiles()).allSatisfy(
                        file -> assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(
                                data.getRecordsInFile(file),
                                "key0", LongStream.range(-10_000, 0))))
                .satisfies(data -> assertThat(data.getNumRecords()).isEqualTo(10_000));
        assertThat(actualActiveData.getPartitionData("right"))
                .satisfies(data -> assertThat(data.getFiles()).allSatisfy(
                        file -> assertThatRecordsHaveFieldValuesThatAllAppearInRangeInSameOrder(
                                data.getRecordsInFile(file),
                                "key0", LongStream.range(0, 10_000))))
                .satisfies(data -> assertThat(data.getNumRecords()).isEqualTo(10_000));
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualActiveData.getFiles(), new LocalFileSystemSketchesStore()))
                .usingComparator(SketchesDecilesComparator.longsMaxDiff(rowListAndSchema.sleeperSchema, 50))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws Exception {
        // Given
        RecordGenerator.RowListAndSchema rowListAndSchema = RecordGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        update(stateStore).initialise(new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                .rootFirst("root")
                .splitToNewChildren("root", "left", "right", 0L)
                .buildList());
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When/Then
        assertThatThrownBy(() -> ingestRecords(rowListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 32 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 32 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 64 * 1024 * 1024L);
        })).isInstanceOf(OutOfMemoryException.class).hasNoSuppressedExceptions();
    }

    private static void ingestRecords(
            RecordGenerator.RowListAndSchema rowListAndSchema, IngestCoordinatorTestParameters parameters,
            Consumer<InstanceProperties> config) throws Exception {
        try (IngestCoordinator<Row> ingestCoordinator = parameters.toBuilder()
                .localDirectWrite().backedByArrow().setInstanceProperties(config).buildCoordinator()) {
            for (Row row : rowListAndSchema.rowList) {
                ingestCoordinator.write(row);
            }
        }
    }
}
