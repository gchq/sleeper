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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionRunner;
import sleeper.compaction.job.execution.testutils.CompactSortedFilesTestBase;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.io.parquet.utils.HadoopConfigurationProvider;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestData.writeRootFile;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestUtils.assignJobIdToInputFiles;
import static sleeper.compaction.job.execution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

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
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        DefaultSelector selector = createCompactionSelector(schema,
                HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        CompactionRunner runner = selector.chooseCompactor(compactionJob);
        RecordsProcessed summary = runner.compact(compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isEqualTo(data.size());
        assertThat(summary.getRecordsWritten()).isEqualTo(data.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data);
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
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        DefaultSelector selector = createCompactionSelector(schema,
                HadoopConfigurationProvider.getConfigurationForECS(instanceProperties));
        CompactionRunner runner = selector.chooseCompactor(compactionJob);
        RecordsProcessed summary = runner.compact(compactionJob);

        // Then
        //  - Read output file and check that it contains the right results
        assertThat(summary.getRecordsRead()).isZero();
        assertThat(summary.getRecordsWritten()).isZero();
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEmpty();
    }
}
