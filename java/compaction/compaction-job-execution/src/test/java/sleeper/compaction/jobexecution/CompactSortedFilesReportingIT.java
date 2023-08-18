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
import org.mockito.InOrder;
import org.mockito.Mockito;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestBase;
import sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestDataHelper;
import sleeper.core.record.Record;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.StateStore;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.testutils.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedSinglePartition;

class CompactSortedFilesReportingIT extends CompactSortedFilesTestBase {

    private final CompactionJobStatusStore jobStatusStore = mock(CompactionJobStatusStore.class);

    @Test
    void shouldReportJobStatusUpdatesWhenCompacting() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        StateStore stateStore = inMemoryStateStoreWithFixedSinglePartition(schema);
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        RecordsProcessedSummary summary =
                createCompactSortedFiles(schema, compactionJob, stateStore, jobStatusStore, DEFAULT_TASK_ID).compact();

        // Then
        InOrder order = Mockito.inOrder(jobStatusStore);
        order.verify(jobStatusStore).jobStarted(eq(compactionJob), any(Instant.class), eq(DEFAULT_TASK_ID));
        order.verify(jobStatusStore).jobFinished(compactionJob, summary, DEFAULT_TASK_ID);
        order.verifyNoMoreInteractions();

        assertThat(summary.getStartTime()).isNotNull();
        assertThat(summary.getFinishTime()).isNotNull();
        assertThat(summary.getDurationInSeconds()).isPositive();
        assertThat(summary.getRecordsReadPerSecond()).isPositive();
        assertThat(summary.getRecordsWrittenPerSecond()).isPositive();
    }

}
