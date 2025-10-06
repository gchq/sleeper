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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

import sleeper.arrow.ArrowRowWriter;
import sleeper.arrow.ArrowRowWriterAcceptingRows;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.ingest.runner.testutils.IngestCoordinatorTestParameters;
import sleeper.ingest.runner.testutils.RowGenerator;
import sleeper.ingest.runner.testutils.TestFilesAndRows;
import sleeper.sketches.store.LocalFileSystemSketchesStore;
import sleeper.sketches.testutils.SketchesDeciles;
import sleeper.sketches.testutils.SketchesDecilesComparator;

import java.util.ArrayList;
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

class IngestCoordinatorUsingDirectWriteBackedByArrowRowWriterAcceptingRowListIT extends DirectWriteBackedByArrowTestBase {

    @Test
    void shouldWriteRowsWhenThereAreMoreInAPartitionThanCanFitInMemory() throws Exception {
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
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
        ingestRows(rowListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 128 * 1024 * 1024L);
        });

        // Then
        TestFilesAndRows actualFileData = TestFilesAndRows.loadFileReferences(stateStore, rowListAndSchema.sleeperSchema, configuration);

        assertThat(actualFileData.getFiles())
                .extracting(FileReference::getPartitionId, FileReference::getFilename)
                .containsExactlyInAnyOrder(
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile.parquet"));

        assertThat(actualFileData.getSetOfAllRows())
                .isEqualTo(new HashSet<>(rowListAndSchema.rowList));
        assertThat(actualFileData.getPartitionData("left").streamAllRows())
                .extracting(row -> row.get("key0"))
                .containsExactlyElementsOf(LongStream.range(-10000, 0).boxed()
                        .collect(Collectors.toList()));
        assertThat(actualFileData.getPartitionData("right").streamAllRows())
                .extracting(row -> row.get("key0"))
                .containsExactlyElementsOf(LongStream.range(0, 10000).boxed()
                        .collect(Collectors.toList()));
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFileData.getFiles(), new LocalFileSystemSketchesStore()))
                .usingComparator(SketchesDecilesComparator.longsMaxDiff(rowListAndSchema.sleeperSchema, 50))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @Test
    void shouldWriteRowsWhenThereAreMoreThanCanFitInLocalFile() throws Exception {
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
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
        ingestRows(rowListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 16 * 1024 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 2 * 1024 * 1024L);
        });

        // Then
        TestFilesAndRows actualFileData = TestFilesAndRows.loadFileReferences(stateStore, rowListAndSchema.sleeperSchema, configuration);

        assertThat(actualFileData.getFiles())
                .extracting(FileReference::getPartitionId, FileReference::getFilename)
                .containsExactlyInAnyOrder(
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile1.parquet"),
                        tuple("left", parameters.getLocalFilePrefix() + "/data/partition_left/leftFile2.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile1.parquet"),
                        tuple("right", parameters.getLocalFilePrefix() + "/data/partition_right/rightFile2.parquet"));
        assertThat(actualFileData.getSetOfAllRows())
                .isEqualTo(new HashSet<>(rowListAndSchema.rowList));
        assertThat(actualFileData.getPartitionData("left"))
                .satisfies(data -> assertThat(data.getFiles()).allSatisfy(
                        file -> assertThatRowsHaveFieldValuesThatAllAppearInRangeInSameOrder(
                                data.getRowsInFile(file),
                                "key0", LongStream.range(-10_000, 0))))
                .satisfies(data -> assertThat(data.getNumRows()).isEqualTo(10_000));
        assertThat(actualFileData.getPartitionData("right"))
                .satisfies(data -> assertThat(data.getFiles()).allSatisfy(
                        file -> assertThatRowsHaveFieldValuesThatAllAppearInRangeInSameOrder(
                                data.getRowsInFile(file),
                                "key0", LongStream.range(0, 10_000))))
                .satisfies(data -> assertThat(data.getNumRows()).isEqualTo(10_000));
        assertThat(SketchesDeciles.fromFileReferences(rowListAndSchema.sleeperSchema, actualFileData.getFiles(), new LocalFileSystemSketchesStore()))
                .usingComparator(SketchesDecilesComparator.longsMaxDiff(rowListAndSchema.sleeperSchema, 50))
                .isEqualTo(SketchesDeciles.from(rowListAndSchema.sleeperSchema, rowListAndSchema.rowList));
    }

    @Test
    void shouldErrorWhenBatchBufferAndWorkingBufferAreSmall() throws Exception {
        RowGenerator.RowListAndSchema rowListAndSchema = RowGenerator.genericKey1D(
                new LongType(),
                LongStream.range(-10000, 10000).boxed().collect(Collectors.toList()));
        tableProperties.setSchema(rowListAndSchema.sleeperSchema);
        update(stateStore).initialise(
                new PartitionsBuilder(rowListAndSchema.sleeperSchema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "left", "right", 0L)
                        .buildList());
        IngestCoordinatorTestParameters parameters = createTestParameterBuilder()
                .fileNames(List.of("leftFile", "rightFile"))
                .build();

        // When
        assertThatThrownBy(() -> ingestRows(rowListAndSchema, parameters, properties -> {
            properties.setNumber(ARROW_INGEST_WORKING_BUFFER_BYTES, 32 * 1024L);
            properties.setNumber(ARROW_INGEST_BATCH_BUFFER_BYTES, 32 * 1024L);
            properties.setNumber(ARROW_INGEST_MAX_LOCAL_STORE_BYTES, 64 * 1024 * 1024L);
        })).isInstanceOf(OutOfMemoryException.class).hasNoSuppressedExceptions();
    }

    private static List<RowList> buildScrambledRowLists(RowGenerator.RowListAndSchema rowListAndSchema) {
        RowList[] rowLists = new RowList[5];
        for (int i = 0; i < rowLists.length; i++) {
            rowLists[i] = new RowList();
        }
        int i = 0;
        for (Row row : rowListAndSchema.rowList) {
            rowLists[i].addRow(row);
            i++;
            if (i == 5) {
                i = 0;
            }
        }
        return List.of(rowLists);
    }

    private static void ingestRows(
            RowGenerator.RowListAndSchema rowListAndSchema, IngestCoordinatorTestParameters parameters,
            Consumer<InstanceProperties> config) throws Exception {
        try (IngestCoordinator<RowList> ingestCoordinator = parameters
                .toBuilder().localDirectWrite().setInstanceProperties(config).build()
                .buildCoordinatorWithArrowWriter(new ArrowRowWriterAcceptingRowList())) {
            for (RowList rowList : buildScrambledRowLists(rowListAndSchema)) {
                ingestCoordinator.write(rowList);
            }
        }
    }

    static class RowList {
        private final List<Row> rows;

        RowList() {
            this.rows = new ArrayList<>();
        }

        public void addRow(Row row) {
            rows.add(row);
        }

        public List<Row> getRows() {
            return rows;
        }
    }

    static class ArrowRowWriterAcceptingRowList implements ArrowRowWriter<RowList> {

        @Override
        public int insert(List<Field> allFields, VectorSchemaRoot vectorSchemaRoot, RowList rowList, int startInsertAtRowNo) {
            int i = 0;
            for (Row row : rowList.getRows()) {
                ArrowRowWriterAcceptingRows.writeRow(
                        allFields, vectorSchemaRoot, row, startInsertAtRowNo + i);
                i++;
            }
            int finalRowCount = startInsertAtRowNo + i;
            vectorSchemaRoot.setRowCount(finalRowCount);
            return finalRowCount;
        }
    }
}
