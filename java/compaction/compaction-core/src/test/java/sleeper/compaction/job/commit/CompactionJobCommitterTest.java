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
package sleeper.compaction.job.commit;

import org.junit.jupiter.api.Test;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.ReplaceRequestsFailedException;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.compaction.job.CompactionJobStatusTestData.jobCreated;

public class CompactionJobCommitterTest extends CompactionJobCommitterTestBase {

    @Test
    void shouldCommitCompactionJobsOnDifferentTables() throws Exception {
        // Given
        TableProperties table1 = createTable();
        TableProperties table2 = createTable();
        FileReference file1 = addInputFile(table1, "file1.parquet", 123);
        FileReference file2 = addInputFile(table2, "file2.parquet", 456);
        CompactionJob job1 = createCompactionJobForOneFileAndAssign(table1, file1, "job-1", Instant.parse("2024-05-01T10:50:00Z"));
        CompactionJob job2 = createCompactionJobForOneFileAndAssign(table2, file2, "job-2", Instant.parse("2024-05-01T10:50:30Z"));
        RecordsProcessedSummary summary1 = new RecordsProcessedSummary(
                new RecordsProcessed(120, 100),
                Instant.parse("2024-05-01T10:58:00Z"), Duration.ofMinutes(1));
        RecordsProcessedSummary summary2 = new RecordsProcessedSummary(
                new RecordsProcessed(450, 400),
                Instant.parse("2024-05-01T10:58:30Z"), Duration.ofMinutes(1));
        CompactionJobCommitRequest commit1 = runCompactionJobOnTask("task-1", job1, summary1);
        CompactionJobCommitRequest commit2 = runCompactionJobOnTask("task-2", job2, summary2);
        stateStore(table1).fixFileUpdateTime(Instant.parse("2024-05-01T11:00:00Z"));
        stateStore(table2).fixFileUpdateTime(Instant.parse("2024-05-01T11:00:30Z"));

        // When
        CompactionJobCommitter jobCommitter = jobCommitter();
        jobCommitter.apply(commit1);
        jobCommitter.apply(commit2);

        // Then
        StateStore state1 = stateStore(table1);
        StateStore state2 = stateStore(table2);
        CompactionJobStatus status1 = statusStore.getJob(job1.getId()).orElseThrow();
        CompactionJobStatus status2 = statusStore.getJob(job2.getId()).orElseThrow();
        assertThat(status1).isEqualTo(jobCreated(job1,
                Instant.parse("2024-05-01T10:50:00Z"),
                finishedCompactionRun("task-1", summary1)));
        assertThat(status2).isEqualTo(jobCreated(job2,
                Instant.parse("2024-05-01T10:50:30Z"),
                finishedCompactionRun("task-2", summary2)));
        assertThat(state1.getFileReferences()).containsExactly(
                fileFactory(table1, Instant.parse("2024-05-01T11:00:00Z"))
                        .rootFile(job1.getOutputFile(), 100));
        assertThat(state2.getFileReferences()).containsExactly(
                fileFactory(table2, Instant.parse("2024-05-01T11:00:30Z"))
                        .rootFile(job2.getOutputFile(), 400));
    }

    @Test
    void shouldFailWhenFileDoesNotExistInStateStore() throws Exception {
        // Given
        createTable();
        FileReference file = inputFileFactory().rootFile("file.parquet", 123);
        CompactionJob job = createCompactionJobForOneFileAndRecordStatus(file);
        CompactionJobCommitRequest commit = runCompactionJobOnTask("test-task", job);

        // When
        assertThatThrownBy(() -> jobCommitter().apply(commit))
                .isInstanceOf(ReplaceRequestsFailedException.class)
                .hasCauseInstanceOf(FileNotFoundException.class);

        // Then
        assertThat(stateStore().getFileReferences()).isEmpty();
        assertThat(foundWaits).isEmpty();
    }

}
