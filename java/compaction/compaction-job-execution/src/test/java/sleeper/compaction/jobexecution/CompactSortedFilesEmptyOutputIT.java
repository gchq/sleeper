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

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.writeRootFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

class CompactSortedFilesEmptyOutputIT extends CompactSortedFilesTestBase {

    @Test
    void shouldMergeFilesCorrectlyWhenSomeAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        FileReference file1 = ingestRecordsGetFile(data);
        FileReference file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        stateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), List.of(file1, file2));

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.run();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isEqualTo(data.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(file1.getFilename(), file2.getFilename());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileReferenceFactory.from(schema, stateStore)
                        .rootFile(compactionJob.getOutputFile(), 100L));
    }

    @Test
    void shouldMergeFilesCorrectlyWhenAllAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        FileReference file1 = writeRootFile(schema, stateStore, dataFolderName + "/file1.parquet", List.of());
        FileReference file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        stateStore.atomicallyUpdateJobStatusOfFiles(compactionJob.getId(), List.of(file1, file2));

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.run();

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isZero();
        assertThat(summary.getRecordsWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();

        // - Check DynamoDBStateStore has correct ready for GC files
        assertThat(stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)))
                .containsExactly(file1.getFilename(), file2.getFilename());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileReferenceFactory.from(schema, stateStore)
                        .rootFile(compactionJob.getOutputFile(), 0L));
    }
}
