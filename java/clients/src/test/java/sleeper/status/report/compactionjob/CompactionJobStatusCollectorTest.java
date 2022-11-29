/*
 * Copyright 2022 Crown Copyright
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

package sleeper.status.report.compactionjob;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobTestDataHelper;
import sleeper.compaction.job.TestCompactionJobStatus;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompactionJobStatusCollectorTest {
    private static final String JOB_ID_NOT_FOUND = "job-id-not-found";
    private final CompactionJobStatusStore statusStore = mock(CompactionJobStatusStore.class);
    private final CompactionJobStatusCollector collector = new CompactionJobStatusCollector(statusStore, "table-test");

    private CompactionJobStatus getJob() {
        CompactionJobTestDataHelper dataHelper = new CompactionJobTestDataHelper();
        CompactionJob job = dataHelper.singleFileCompaction();
        return TestCompactionJobStatus.created(job, Instant.now());
    }

    @Test
    public void shouldReturnListOfJobStatusWithNoJobsFound() {
        // Given
        List<String> jobIds = Collections.singletonList(JOB_ID_NOT_FOUND);
        when(statusStore.getJob(JOB_ID_NOT_FOUND)).thenReturn(Optional.empty());

        // When
        List<CompactionJobStatus> statusList = collector.runDetailedQuery(jobIds);

        // Then
        assertThat(statusList)
                .isEmpty();
    }

    @Test
    public void shouldReturnListOfJobStatusWithJobFound() {
        // Given
        CompactionJobStatus jobStatus = getJob();
        List<String> jobIds = Collections.singletonList(jobStatus.getJobId());
        when(statusStore.getJob(jobStatus.getJobId())).thenReturn(Optional.of(jobStatus));

        // When
        List<CompactionJobStatus> statusList = collector.runDetailedQuery(jobIds);

        // Then
        assertThat(statusList)
                .containsExactly(jobStatus);
    }

    @Test
    public void shouldReturnListOfJobStatusWithOneJobNotFound() {
        // Given
        CompactionJobStatus jobStatus = getJob();
        List<String> jobIds = Arrays.asList(JOB_ID_NOT_FOUND, jobStatus.getJobId());
        when(statusStore.getJob(jobStatus.getJobId())).thenReturn(Optional.of(jobStatus));
        when(statusStore.getJob(JOB_ID_NOT_FOUND)).thenReturn(Optional.empty());

        // When
        List<CompactionJobStatus> statusList = collector.runDetailedQuery(jobIds);

        // Then
        assertThat(statusList)
                .containsExactly(jobStatus);
    }
}
