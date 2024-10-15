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
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestBase;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestData;
import sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils;
import sleeper.core.iterator.impl.AgeOffIterator;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.sketches.testutils.SketchesDeciles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.compaction.job.execution.testutils.CompactionRunnerTestUtils.assignJobIdToInputFiles;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;

class JavaCompactionRunnerIteratorIT extends CompactionRunnerTestBase {

    @Test
    void shouldApplyIteratorDuringCompaction() throws Exception {
        // Given
        Schema schema = CompactionRunnerTestUtils.createSchemaWithKeyTimestampValue();
        tableProperties.setSchema(schema);
        stateStore.initialise(new PartitionsBuilder(schema).singlePartition("root").buildList());

        List<Record> data1 = CompactionRunnerTestData.specifiedFromEvens((even, record) -> {
            record.put("key", (long) even);
            record.put("timestamp", System.currentTimeMillis());
            record.put("value", 987654321L);
        });
        List<Record> data2 = CompactionRunnerTestData.specifiedFromOdds((odd, record) -> {
            record.put("key", (long) odd);
            record.put("timestamp", 0L);
            record.put("value", 123456789L);
        });
        FileReference file1 = ingestRecordsGetFile(data1);
        FileReference file2 = ingestRecordsGetFile(data2);

        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "timestamp,1000000");

        CompactionJob compactionJob = compactionFactory().createCompactionJob(List.of(file1, file2), "root");
        assignJobIdToInputFiles(stateStore, compactionJob);

        // When
        RecordsProcessed summary = compact(schema, compactionJob);

        // Then
        //  - Read output files and check that they contain the right results
        assertThat(summary.getRecordsRead()).isEqualTo(200L);
        assertThat(summary.getRecordsWritten()).isEqualTo(100L);
        assertThat(CompactionRunnerTestData.readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(data1);
        assertThat(SketchesDeciles.from(readSketches(schema, compactionJob.getOutputFile())))
                .isEqualTo(SketchesDeciles.builder()
                        .field("key", deciles -> deciles
                                .min(0L).max(198L)
                                .rank(0.1, 18L).rank(0.2, 38L).rank(0.3, 58L)
                                .rank(0.4, 78L).rank(0.5, 98L).rank(0.6, 118L)
                                .rank(0.7, 138L).rank(0.8, 158L).rank(0.9, 178L))
                        .build());
    }
}
