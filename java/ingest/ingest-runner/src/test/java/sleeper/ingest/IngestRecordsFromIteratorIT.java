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

package sleeper.ingest;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.table.TableProperty.INGEST_FILE_WRITING_STRATEGY;
import static sleeper.configuration.properties.validation.IngestFileWritingStrategy.ONE_FILE_PER_LEAF;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSingleRecord;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;

class IngestRecordsFromIteratorIT extends IngestRecordsTestBase {

    @Test
    void shouldWriteMultipleRecords() throws Exception {
        // Given
        tableProperties.setEnum(INGEST_FILE_WRITING_STRATEGY, ONE_FILE_PER_LEAF);
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 2L)
                        .buildList());

        // When
        long numWritten = ingestFromRecordIterator(stateStore, getRecords().iterator()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getRecords().size());
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
        //  - Read files and check they have the correct records
        FileReference leftFile = fileReferences.get(0);
        FileReference rightFile = fileReferences.get(1);
        assertThat(readRecords(leftFile))
                .containsExactly(getRecords().get(0));
        assertThat(readRecords(rightFile))
                .containsExactly(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct
        assertThat(SketchesDeciles.from(getSketches(schema, leftFile.getFilename())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", builder -> builder
                                .min(1L).max(1L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
        assertThat(SketchesDeciles.from(getSketches(schema, rightFile.getFilename())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", builder -> builder
                                .min(3L).max(3L)
                                .rank(0.1, 3L).rank(0.2, 3L).rank(0.3, 3L)
                                .rank(0.4, 3L).rank(0.5, 3L).rank(0.6, 3L)
                                .rank(0.7, 3L).rank(0.8, 3L).rank(0.9, 3L))
                        .build());
    }

    @Test
    void shouldWriteSingleRecord() throws Exception {
        // Given
        StateStore stateStore = inMemoryStateStoreWithFixedPartitions(
                new PartitionsBuilder(schema)
                        .rootFirst("root")
                        .splitToNewChildren("root", "L", "R", 2L)
                        .buildList());

        // When
        long numWritten = ingestFromRecordIterator(schema, stateStore, getSingleRecord().iterator()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isEqualTo(getSingleRecord().size());
        //  - Check StateStore has correct information
        FileReferenceFactory fileReferenceFactory = FileReferenceFactory.from(stateStore);
        List<FileReference> fileReferences = stateStore.getFileReferences()
                .stream()
                .sorted(Comparator.comparing(FileReference::getPartitionId))
                .collect(Collectors.toList());
        assertThat(fileReferences)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileReferenceFactory.partitionFile("L", 1L));
        //  - Read files and check they have the correct records
        assertThat(readRecords(fileReferences.get(0)))
                .containsExactly(getSingleRecord().get(0));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        assertThat(SketchesDeciles.from(getSketches(schema, fileReferences.get(0).getFilename())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", builder -> builder
                                .min(1L).max(1L)
                                .rank(0.1, 1L).rank(0.2, 1L).rank(0.3, 1L)
                                .rank(0.4, 1L).rank(0.5, 1L).rank(0.6, 1L)
                                .rank(0.7, 1L).rank(0.8, 1L).rank(0.9, 1L))
                        .build());
    }

    @Test
    void shouldWriteNoRecordsWhenIteratorIsEmpty() throws Exception {
        // Given
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);

        // When
        long numWritten = ingestFromRecordIterator(schema, stateStore, Collections.emptyIterator()).getRecordsWritten();

        // Then:
        //  - Check the correct number of records were written
        assertThat(numWritten).isZero();
        //  - Check StateStore has correct information
        assertThat(stateStore.getFileReferences()).isEmpty();
    }
}
