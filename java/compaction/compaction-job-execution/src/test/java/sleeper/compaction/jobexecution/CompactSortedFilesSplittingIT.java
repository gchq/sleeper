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
package sleeper.compaction.jobexecution;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
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
    void shouldCreateReferencesForFileInChildPartitions() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema);
        stateStore.initialise(partitions.singlePartition("root").buildList());

        List<Record> records = List.of(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 7L)));
        FileReference rootFile = ingestRecordsGetFile(records);
        Sketches rootSketches = getSketches(schema, rootFile);
        partitions.splitToNewChildren("root", "L", "R", 5L)
                .applySplit(stateStore, "root");
        tableProperties.set(PARTITION_SPLIT_THRESHOLD, "1");

        CompactionJob compactionJob = createCompactionJob();

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then the new files are recorded in the state store
        List<FileReference> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        SplitFileReference.referenceForChildPartition(rootFile, "L"),
                        SplitFileReference.referenceForChildPartition(rootFile, "R"));

        // And the new files each have all the copied records and sketches
        assertThat(activeFiles).allSatisfy(file -> {
            assertThat(readDataFile(schema, file)).isEqualTo(records);
            assertThat(asDecilesMaps(getSketches(schema, file)))
                    .isEqualTo(asDecilesMaps(rootSketches));
        });

        // And the original file is not marked as ready for GC
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .isEmpty();

        // And we see no records were read or written
        assertThat(summary.getRecordsRead()).isZero();
        assertThat(summary.getRecordsWritten()).isZero();
    }

    @Test
    void shouldCreateCopiesOfFileInChildPartitions() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema);
        stateStore.initialise(partitions.singlePartition("root").buildList());

        List<Record> records = List.of(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 7L)));
        FileReference rootFile = ingestRecordsGetFile(records);
        Sketches rootSketches = getSketches(schema, rootFile);
        partitions.splitToNewChildren("root", "L", "R", 5L)
                .applySplit(stateStore, "root");
        tableProperties.set(PARTITION_SPLIT_THRESHOLD, "1");

        CompactionJob compactionJob = createCompactionJob();

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compactByCopy();

        // Then the new files are recorded in the state store
        List<FileReference> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        SplitFileReference.copyToChildPartition(rootFile, "L",
                                jobPartitionFilename(compactionJob, "L", 0)),
                        SplitFileReference.copyToChildPartition(rootFile, "R",
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

        FileReference rootFile = ingestRecordsGetFile(List.of(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 7L))));
        partitions.splitToNewChildren("root", "L", "R", 5L)
                .applySplit(stateStore, "root");

        CompactionJob splittingJob = compactionFactory()
                .createSplittingCompactionJob(List.of(rootFile), "root", "L", "R");
        createCompactSortedFiles(schema, splittingJob).compact();
        FileReference leftFile1 = firstFileInPartition(stateStore.getActiveFiles(), "L");
        FileReference leftFile2 = ingestRecordsGetFile(List.of(new Record(Map.of("key", 4L))));

        // When
        CompactionJob compactionJob = compactionFactory()
                .createCompactionJob(List.of(leftFile1, leftFile2), "L");
        stateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), List.of(leftFile1, leftFile2));
        RecordsProcessedSummary summary = createCompactSortedFiles(schema, compactionJob).compact();

        // Then the new file is recorded in the state store
        List<FileReference> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        FileReferenceFactory.from(partitions.buildTree())
                                .partitionFile("L", jobPartitionFilename(compactionJob, "L"), 2),
                        SplitFileReference.referenceForChildPartition(rootFile, "R"));

        // And the new file has all the records and sketches
        FileReference foundLeft = firstFileInPartition(activeFiles, "L");
        assertThat(readDataFile(schema, foundLeft)).containsExactly(
                new Record(Map.of("key", 3L)),
                new Record(Map.of("key", 4L)));
        assertThat(asDecilesMaps(getSketches(schema, foundLeft)))
                .isEqualTo(Map.of("key", decilesMap(
                        3L, 3L, 3L, 3L, 3L,
                        4L, 4L, 4L, 4L, 4L, 4L)));

        // And the second left file is ready for GC (that file has no more references)
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(leftFile2.getFilename());

        // And we see the records were read and written
        assertThat(summary.getRecordsRead()).isEqualTo(2L);
        assertThat(summary.getRecordsWritten()).isEqualTo(2L);
    }

    @Test
    void shouldCreateReferencesForFileInChildPartitionsWhenReferencesAlreadyExist() throws Exception {
        // Given
        Schema schema = schemaWithKey("key", new LongType());
        PartitionsBuilder partitions = new PartitionsBuilder(schema);
        stateStore.initialise(partitions.singlePartition("root").buildList());
        tableProperties.set(PARTITION_SPLIT_THRESHOLD, "1");

        List<Record> records = List.of(
                new Record(Map.of("key", 4L)),
                new Record(Map.of("key", 8L)),
                new Record(Map.of("key", 12L)),
                new Record(Map.of("key", 16L)));
        FileReference rootFile = ingestRecordsGetFile(records);
        Sketches rootSketches = getSketches(schema, rootFile);
        // - Perform first split to create references to root file
        partitions.splitToNewChildren("root", "L", "R", 10L)
                .applySplit(stateStore, "root");
        CompactionJob compactionJob = createCompactionJob();
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary1 = compactSortedFiles.compact();

        // When we perform another split
        partitions.splitToNewChildren("L", "LL", "LR", 5L)
                .applySplit(stateStore, "L");
        CompactionJob compactionJob2 = createCompactionJob();
        CompactSortedFiles compactSortedFiles2 = createCompactSortedFiles(schema, compactionJob2);
        RecordsProcessedSummary summary2 = compactSortedFiles2.compact();

        // Then the new files are recorded in the state store
        List<FileReference> activeFiles = stateStore.getActiveFiles();
        assertThat(activeFiles)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrder(
                        SplitFileReference.referenceForChildPartition(rootFile, "LL", 1L),
                        SplitFileReference.referenceForChildPartition(rootFile, "LR", 1L),
                        SplitFileReference.referenceForChildPartition(rootFile, "R", 2L));

        // And the new files each have all the copied records and sketches
        assertThat(activeFiles).allSatisfy(file -> {
            assertThat(readDataFile(schema, file)).isEqualTo(records);
            assertThat(asDecilesMaps(getSketches(schema, file)))
                    .isEqualTo(asDecilesMaps(rootSketches));
        });

        // And the original file is not marked as ready for GC
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .isEmpty();

        // And we see no records were read or written
        assertThat(summary1.getRecordsRead()).isZero();
        assertThat(summary1.getRecordsWritten()).isZero();
        assertThat(summary2.getRecordsRead()).isZero();
        assertThat(summary2.getRecordsWritten()).isZero();
    }

    private FileReference firstFileInPartition(List<FileReference> files, String partitionId) {
        return files.stream()
                .filter(file -> Objects.equals(partitionId, file.getPartitionId()))
                .findFirst().orElseThrow();
    }
}
