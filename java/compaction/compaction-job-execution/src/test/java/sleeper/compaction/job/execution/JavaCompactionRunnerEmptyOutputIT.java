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
package sleeper.compaction.job.execution;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.tracker.job.RecordsProcessed;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestData.readDataFile;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestData.writeRootFile;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.createSchemaWithTypesForKeyAndTwoValues;

class JavaCompactionRunnerEmptyOutputIT extends CompactionRunnerTestBase {

    @Test
    void shouldMergeFilesWhenSomeAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data = keyAndTwoValuesSortedEvenLongs();
        FileReference file1 = ingestRecordsGetFile(data);
        FileReference file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(schema, compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isEqualTo(data.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);
    }

    @Test
    void shouldMergeFilesWhenAllAreEmpty() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        FileReference file1 = writeRootFile(schema, stateStore, dataFolderName + "/file1.parquet", List.of());
        FileReference file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(schema, compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isZero();
        assertThat(summary.getRecordsWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();
    }

    @Test
    void shouldWriteSketchWhenWritingEmptyFile() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        FileReference file1 = writeRootFile(schema, stateStore, dataFolderName + "/file1.parquet", List.of());
        FileReference file2 = writeRootFile(schema, stateStore, dataFolderName + "/file2.parquet", List.of());

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        compact(schema, compactionJob);

        // Then
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.builder()
                        .fieldEmpty("key")
                        .build());
    }
}
