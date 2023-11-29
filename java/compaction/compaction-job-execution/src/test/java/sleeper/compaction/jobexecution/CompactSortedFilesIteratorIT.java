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
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.specifiedFromEvens;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.specifiedFromOdds;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;

class CompactSortedFilesIteratorIT extends CompactSortedFilesTestBase {

    @Test
    void shouldApplyIteratorDuringStandardCompaction() throws Exception {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data1 = specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        FileInfo file1 = ingestRecordsGetFile(data1);
        FileInfo file2 = ingestRecordsGetFile(data2);

        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(100L);
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data1);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, List.of(file1, file2));

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(FileInfoFactory.from(schema, stateStore)
                        .rootFile(compactionJob.getOutputFile(), 100L));
    }

    // Need to fix assertions on output files
    @Disabled("TODO")
    @Test
    void shouldNotApplyIteratorDuringSplittingCompaction() throws Exception {
        // Given
        Schema schema = CompactSortedFilesTestUtils.createSchemaWithKeyTimestampValue();
        tableProperties.setSchema(schema);
        PartitionsBuilder partitions = new PartitionsBuilder(schema).rootFirst("A");
        stateStore.initialise(partitions.buildList());

        List<Record> data1 = specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        FileInfo file1 = ingestRecordsGetFile(data1);
        FileInfo file2 = ingestRecordsGetFile(data2);

        partitions.splitToNewChildren("A", "B", "C", 200L)
                .applySplit(stateStore, "A");

        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");


        CompactionJob compactionJob = compactionFactory().createSplittingCompactionJob(
                List.of(file1, file2), "A", "B", "C", 100L, 0);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob);
        RecordsProcessedSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(400L);
        assertThat(summary.getRecordsWritten()).isEqualTo(400L);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getLeft())).isEqualTo(data1);
        assertThat(readDataFile(schema, compactionJob.getOutputFiles().getRight())).isEqualTo(data1);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, List.of(file1, file2));

        // - Check DynamoDBStateStore has correct active files
        FileInfoFactory fileInfoFactory = FileInfoFactory.from(schema, stateStore);
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(
                        fileInfoFactory.rootFile(compactionJob.getOutputFiles().getLeft(), 200L),
                        fileInfoFactory.rootFile(compactionJob.getOutputFiles().getRight(), 200L));
    }
}
