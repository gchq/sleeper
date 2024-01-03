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
import sleeper.core.statestore.SplitFileInfo;
import sleeper.sketches.Sketches;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.ingest.testutils.IngestRecordsTestDataHelper.getSketches;
import static sleeper.sketches.testutils.AssertQuantiles.asDecilesMaps;
import static sleeper.sketches.testutils.AssertQuantiles.decilesMap;

class CompactSortedFilesSplittingIT extends CompactSortedFilesTestBase {

    @Test
    void shouldCopyAFileToChildPartitions() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema);
        stateStore.initialise(partitions.singlePartition("root").buildList());

        List<Record> records = List.of(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 7L)));
        FileInfo rootFile = ingestRecordsGetFile(records);
        Sketches rootSketches = getSketches(schema, rootFile);
        partitions.splitToNewChildren("root", "L", "R", 5L)
                .applySplit(stateStore, "root");
        tableProperties.set(PARTITION_SPLIT_THRESHOLD, "1");

        CompactionJob compactionJob = createCompactionJob();

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then the new files are recorded in the state store
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        SplitFileInfo.copyToChildPartition(rootFile, "L",
                                jobPartitionFilename(compactionJob, "L", 0)),
                        SplitFileInfo.copyToChildPartition(rootFile, "R",
                                jobPartitionFilename(compactionJob, "R", 0)));

        // And the new files each have all the copied records and sketches
        assertThat(activeFiles).allSatisfy(file -> {
            assertThat(readDataFile(schema, file)).isEqualTo(records);
            assertThat(asDecilesMaps(getSketches(schema, file)))
                    .isEqualTo(asDecilesMaps(rootSketches));
        });

        // And the original file is ready for GC
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(rootFile.getFilename());

        // And we see the records were read and written twice
        assertThat(summary.getRecordsRead()).isEqualTo(4L);
        assertThat(summary.getRecordsWritten()).isEqualTo(4L);
    }

    @Test
    void shouldExcludeRecordsNotInPartitionWhenPerformingStandardCompaction() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema);
        stateStore.initialise(partitions.singlePartition("root").buildList());

        FileInfo rootFile = ingestRecordsGetFile(List.of(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 7L))));
        partitions.splitToNewChildren("root", "L", "R", 5L)
                .applySplit(stateStore, "root");

        CompactionJob splittingJob = compactionFactory()
                .createSplittingCompactionJob(List.of(rootFile), "root", "L", "R");
        createCompactSortedFiles(schema, splittingJob).compact();
        FileInfo leftFile1 = firstFileInPartition(stateStore.getActiveFiles(), "L");
        FileInfo leftFile2 = ingestRecordsGetFile(List.of(new Record(Map.of("key", 4L))));

        // When
        CompactionJob compactionJob = compactionFactory()
                .createCompactionJob(List.of(leftFile1, leftFile2), "L");
        RecordsProcessedSummary summary = createCompactSortedFiles(schema, compactionJob).compact();

        // Then the new file is recorded in the state store
        List<FileInfo> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        FileInfoFactory.from(partitions.buildTree())
                                .partitionFile("L", jobPartitionFilename(compactionJob, "L"), 2),
                        SplitFileInfo.copyToChildPartition(rootFile, "R",
                                jobPartitionFilename(splittingJob, "R", 0)));

        // And the new file has all the copied records and sketches
        FileInfo foundLeft = firstFileInPartition(activeFiles, "L");
        assertThat(readDataFile(schema, foundLeft)).containsExactly(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 4L)));
        assertThat(asDecilesMaps(getSketches(schema, foundLeft)))
                .isEqualTo(Map.of("key", decilesMap(
                        3L, 3L, 3L, 3L, 3L,
                        4L, 4L, 4L, 4L, 4L, 4L)));

        // And the original files are ready for GC
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(rootFile.getFilename(), leftFile1.getFilename(), leftFile2.getFilename());

        // And we see the records were read and written
        assertThat(summary.getRecordsRead()).isEqualTo(2L);
        assertThat(summary.getRecordsWritten()).isEqualTo(2L);
    }

    private FileInfo firstFileInPartition(List<FileInfo> files, String partitionId) {
        return files.stream()
                .filter(file -> Objects.equals(partitionId, file.getPartitionId()))
                .findFirst().orElseThrow();
    }
}
