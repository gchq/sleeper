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
package sleeper.compaction.jobexecution;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.writeRootFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

class CompactSortedFilesEmptyOutputIT extends CompactSortedFilesTestBase {

    @Test
    void shouldMergeFilesCorrectlyWhenSomeAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        FileInfo file1 = ingestRecordsGetFile(data);
        FileInfo file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isEqualTo(data.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, List.of(file1, file2));

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileInfoFactory.from(schema, stateStore)
                        .rootFile(compactionJob.getOutputFile(), 100L));
    }

    @Test
    void shouldMergeFilesCorrectlyWhenAllAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        FileInfo file1 = writeRootFile(schema, stateStore, dataFolderName + "/file1.parquet", List.of());
        FileInfo file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isZero();
        assertThat(summary.getRecordsWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, List.of(file1, file2));

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileInfoFactory.from(schema, stateStore)
                        .rootFile(compactionJob.getOutputFile(), 0L));
    }

    // Need to fix assertions on output files
    @Disabled("TODO")
    @Test
    void shouldSplitFilesCorrectlyWhenOneFileIsEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("A");
        stateStore.initialise(partitions.buildList());

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        FileInfo file1 = ingestRecordsGetFile(data);
        FileInfo file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        partitions.splitToNewChildren("A", "B", "C", 200L)
                .applySplit(stateStore, "A");

        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                List.of(file1, file2), "A", "B", "C", 200L, 0);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(200L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(data);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, List.of(file1, file2));

        // - Check DynamoDBStateStore has correct active files
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(schema, stateStore);
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        fileInfoFactory.partitionFile("B", compactionJob.getOutputFiles().getLeft(), 200L),
                        fileInfoFactory.partitionFile("C", compactionJob.getOutputFiles().getRight(), 0L));
    }
}
