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

package sleeper.status.report.compactionjobstatus;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.core.partition.Partition;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class StatusReporterDetailedQueryTest extends StatusReporterTest {
    @Test
    public void shouldReportCompactionJobStatusCreated() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");

        // When
        CompactionJobStatus status = jobCreated(job, creationTime);

        // Then
        assertThat(statusReporter.report(Collections.singletonList(status), CompactionJobStatusReporter.QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobCreated.txt")
                        .replace("$(jobId)", job.getId()));
    }

    @Test
    public void shouldReportCompactionJobStatusStarted() throws Exception {
        // Given
        Partition partition = dataHelper.singlePartition();
        CompactionJob job = dataHelper.singleFileCompaction(partition);
        Instant creationTime = Instant.parse("2022-09-22T13:33:12.001Z");
        Instant startedTime = Instant.parse("2022-09-22T13:34:12.001Z");
        Instant startedUpdateTime = Instant.parse("2022-09-22T13:39:12.001Z");

        // When
        CompactionJobStatus status = jobStarted(job, creationTime, startedTime, startedUpdateTime);

        // Then
        assertThat(statusReporter.report(Collections.singletonList(status), CompactionJobStatusReporter.QueryType.DETAILED))
                .isEqualTo(example("reports/compactionjobstatus/standard/detailed/standardJobStarted.txt")
                        .replace("$(jobId)", job.getId()));
    }
}
