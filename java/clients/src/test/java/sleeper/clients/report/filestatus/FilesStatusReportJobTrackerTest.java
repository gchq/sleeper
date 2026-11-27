/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report.filestatus;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.tracker.job.run.JobRunTime;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static sleeper.compaction.core.job.CompactionJobAssignmentFromJobTestHelper.assignJobFiles;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.failedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.finishedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.startedCompactionRun;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.uncommittedCompactionRun;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;

public class FilesStatusReportJobTrackerTest extends FilesStatusReportTestBase {
    private final PartitionTree partitions = new PartitionsBuilder(schema)
            .rootFirst("root")
            .splitToNewChildren("root", "left", "right", "mmm")
            .buildTree();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);

    @Test
    void shouldReportJobsByTrackerStatus() {
        // Given files assigned to compaction jobs, some of them more than 12 hours before the report
        setTime(Instant.parse("2022-08-22T00:00:00Z"));
        update(stateStore).initialise(partitions);
        update(stateStore).addFiles(List.of(
                fileFactory.partitionFile("left", "created.parquet", 100),
                fileFactory.partitionFile("left", "failed-1.parquet", 100),
                fileFactory.partitionFile("right", "failed-2.parquet", 100),
                fileFactory.partitionFile("right", "in-progress.parquet", 100),
                fileFactory.partitionFile("left", "uncommitted.parquet", 100),
                fileFactory.partitionFile("right", "finished.parquet", 100),
                fileFactory.partitionFile("right", "untracked.parquet", 100)));
        CompactionJob createdJob = job("created-job", "created.parquet", "left");
        CompactionJob failedJob1 = job("failed-job-1", "failed-1.parquet", "left");
        CompactionJob failedJob2 = job("failed-job-2", "failed-2.parquet", "right");
        CompactionJob inProgressJob = job("in-progress-job", "in-progress.parquet", "right");
        CompactionJob uncommittedJob = job("uncommitted-job", "uncommitted.parquet", "left");
        CompactionJob finishedJob = job("finished-job", "finished.parquet", "right");
        CompactionJob untrackedJob = job("untracked-job", "untracked.parquet", "right");
        setTime(Instant.parse("2022-08-22T01:00:00Z"));
        update(stateStore).assignJobIds(assignJobFiles(List.of(failedJob1, inProgressJob, untrackedJob)));
        setTime(Instant.parse("2022-08-22T14:00:00Z"));
        update(stateStore).assignJobIds(assignJobFiles(List.of(createdJob, failedJob2, uncommittedJob, finishedJob)));

        // And the jobs are in the tracker with each status, except one
        Instant oldCreatedTime = Instant.parse("2022-08-22T01:00:10Z");
        Instant oldStartTime = Instant.parse("2022-08-22T01:01:00Z");
        Instant recentCreatedTime = Instant.parse("2022-08-22T14:00:10Z");
        Instant recentStartTime = Instant.parse("2022-08-22T14:01:00Z");
        setJobTrackerData(
                compactionJobCreated(createdJob, recentCreatedTime),
                compactionJobCreated(failedJob1, oldCreatedTime,
                        failedCompactionRun("test-task", new JobRunTime(oldStartTime, Duration.ofMinutes(1)), List.of("Some failure"))),
                compactionJobCreated(failedJob2, recentCreatedTime,
                        failedCompactionRun("test-task", new JobRunTime(recentStartTime, Duration.ofMinutes(1)), List.of("Some failure"))),
                compactionJobCreated(inProgressJob, oldCreatedTime,
                        startedCompactionRun("test-task", oldStartTime)),
                compactionJobCreated(uncommittedJob, recentCreatedTime,
                        uncommittedCompactionRun("test-task", summary(recentStartTime, Duration.ofMinutes(1), 100, 100))),
                compactionJobCreated(finishedJob, recentCreatedTime,
                        finishedCompactionRun("test-task", summary(recentStartTime, Duration.ofMinutes(1), 100, 100), recentStartTime.plus(Duration.ofMinutes(2)))));

        // When / Then
        Approvals.verify(standardReportString(FileStatusDetailLevel.COMPACTION_JOBS),
                new Options().forFile().withBaseName("jobTrackerStatuses"));
        Approvals.verify(jsonReportString(FileStatusDetailLevel.COMPACTION_JOBS),
                new Options().forFile().withName("jobTrackerStatuses", ".json"));
    }

    private CompactionJob job(String jobId, String filename, String partitionId) {
        return jobFactory.createCompactionJobWithFilenames(jobId, List.of(filename), partitionId);
    }
}
