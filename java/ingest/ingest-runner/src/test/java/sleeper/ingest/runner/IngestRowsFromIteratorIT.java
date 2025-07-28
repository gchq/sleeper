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

package sleeper.ingest.runner;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.model.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.getRows;
import static sleeper.ingest.runner.testutils.IngestRowsTestDataHelper.getSingleRow;

class IngestRowsFromIteratorIT extends IngestRowsTestBase {

    @Test
    void shouldWriteMultipleRows() throws Exception {
        // Given
        tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 2L)
                .buildList());

        // When
        long numWritten = ingestFromRowIterator(stateStore, getRows().iterator()).getRowsWritten();

        // Then:
        //  - Check the correct number of rows were written
        assertThat(numWritten).isEqualTo(getRows().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences()
                .stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileReferenceFactory.partitionFile("L", 1L),
                        fileReferenceFactory.partitionFile("R", 1L));
        //  - Read files and check they have the correct rows
        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        assertThat(readRows(leftFile))
                .containsExactly(getRows().get(0));
        assertThat(readRows(rightFile))
                .containsExactly(getRows().get(1));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, leftFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(1L).max(1L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
        assertThat(SketchesDeciles.fromFile(schema, rightFile, sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(3L).max(3L)
                                .rank(0.1, 3L).rank(0.2, 3L).rank(0.3, 3L)
                                .rank(0.4, 3L).rank(0.5, 3L).rank(0.6, 3L)
                                .rank(0.7, 3L).rank(0.8, 3L).rank(0.9, 3L))
                        .build());
    }

    @Test
    void shouldWriteSingleRow() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .rootFirst("root")
                .splitToNewChildren("root", "L", "R", 2L)
                .buildList());

        // When
        long numWritten = ingestFromRowIterator(stateStore, getSingleRow().iterator()).getRowsWritten();

        // Then:
        //  - Check the correct number of rows were written
        assertThat(numWritten).isEqualTo(getSingleRow().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences()
                .stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.partitionFile("L", 1L));
        //  - Read files and check they have the correct rows
        assertThat(readRows(fileReferences.get(0)))
                .containsExactly(getSingleRow().get(0));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.fromFile(schema, fileReferences.get(0), sketchesStore))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(1L).max(1L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
    }

    @Test
    void shouldWriteNoRowsWhenIteratorIsEmpty() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore(new PartitionsBuilder(schema)
                .singlePartition("root")
                .buildList());

        // When
        long numWritten = ingestFromRowIterator(stateStore, Collections.emptyIterator()).getRowsWritten();

        // Then:
        //  - Check the correct number of rows were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        assertThat(stateStore.getFileReferences()).isEmpty();
    }

    private StateStore initialiseStateStore(List<Partition> partitions) {
        return InMemoryTransactionLogStateStore
                .createAndInitialiseWithPartitions(partitions, tableProperties, new InMemoryTransactionLogs());
    }
}
