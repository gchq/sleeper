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

package sleeper.ingest;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;
import sleeper.ingest.testutils.AssertQuantiles;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getRecords;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSingleRecord;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;

class IngestRecordsFromIteratorIT extends IngestRecordsTestBase {

    @Test
    void shouldWriteMultipleRecords() throws Exception {
        // Given
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
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(schema, stateStore);
        List<FileInfo> activeFiles = stateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getPartitionId))
                .collect(Collectors.toList());
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(
                        fileInfoFactory.partitionFile("L", 1L),
                        fileInfoFactory.partitionFile("R", 1L));
        //  - Read files and check they have the correct records
        FileInfo leftFile = activeFiles.get(0);
        FileInfo rightFile = activeFiles.get(1);
        assertThat(readRecords(leftFile))
                .containsExactly(getRecords().get(0));
        assertThat(readRecords(rightFile))
                .containsExactly(getRecords().get(1));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, leftFile.getFilename()).getQuantilesSketch("key"))
                .min(1L).max(1L)
                .quantile(0.0, 1L).quantile(0.1, 1L)
                .quantile(0.2, 1L).quantile(0.3, 1L)
                .quantile(0.4, 1L).quantile(0.5, 1L)
                .quantile(0.6, 1L).quantile(0.7, 1L)
                .quantile(0.8, 1L).quantile(0.9, 1L).verify();
        AssertQuantiles.forSketch(getSketches(schema, rightFile.getFilename()).getQuantilesSketch("key"))
                .min(3L).max(3L)
                .quantile(0.0, 3L).quantile(0.1, 3L)
                .quantile(0.2, 3L).quantile(0.3, 3L)
                .quantile(0.4, 3L).quantile(0.5, 3L)
                .quantile(0.6, 3L).quantile(0.7, 3L)
                .quantile(0.8, 3L).quantile(0.9, 3L).verify();
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
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(schema, stateStore);
        List<FileInfo> activeFiles = stateStore.getActiveFiles()
                .stream()
                .sorted(Comparator.comparing(FileInfo::getPartitionId))
                .collect(Collectors.toList());
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("filename", "lastStateStoreUpdateTime")
                .containsExactly(fileInfoFactory.partitionFile("L", 1L));
        //  - Read files and check they have the correct records
        assertThat(readRecords(activeFiles.get(0)))
                .containsExactly(getSingleRecord().get(0));
        //  - Check quantiles sketches have been written and are correct (NB the sketches are stochastic so may not be identical)
        AssertQuantiles.forSketch(getSketches(schema, activeFiles.get(0).getFilename()).getQuantilesSketch("key"))
                .min(1L).max(1L)
                .quantile(0.0, 1L).quantile(0.1, 1L)
                .quantile(0.2, 1L).quantile(0.3, 1L)
                .quantile(0.4, 1L).quantile(0.5, 1L)
                .quantile(0.6, 1L).quantile(0.7, 1L)
                .quantile(0.8, 1L).quantile(0.9, 1L).verify();
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
        assertThat(stateStore.getActiveFiles()).isEmpty();
    }
}
