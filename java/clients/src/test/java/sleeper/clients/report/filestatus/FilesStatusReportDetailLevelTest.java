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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.statestore.FileReferenceFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static sleeper.compaction.core.job.CompactionJobAssignmentFromJobTestHelper.assignJobFiles;
import static sleeper.compaction.core.job.CompactionJobStatusFromJobTestData.compactionJobCreated;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionCommittedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionFinishedStatus;
import static sleeper.core.tracker.compaction.job.CompactionJobStatusTestData.compactionStartedStatus;
import static sleeper.core.tracker.job.run.JobRunSummaryTestHelper.summary;
import static sleeper.core.tracker.job.run.JobRunTestData.jobRunOnTask;

public class FilesStatusReportDetailLevelTest extends FilesStatusReportTestBase {
    PartitionTree partitions = new PartitionsBuilder(schema)
            .rootFirst("root")
            .splitToNewChildren("root", "L", "R", "mmm")
            .splitToNewChildren("R", "RL", "RR", "ppp")
            .buildTree();

    @BeforeEach
    void setUp() {
        setTime(Instant.parse("2022-08-22T14:20:00.001Z"));
        update(stateStore).initialise(partitions);
        FileReferenceFactory fileFactory = FileReferenceFactory.from(partitions);
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
        update(stateStore).addFiles(List.of(
                fileFactory.partitionFile("L", "left.parquet", 100),
                fileFactory.partitionFile("RL", "rl-no-compaction.parquet", 200),
                fileFactory.partitionFile("RL", "rl-compaction-1a.parquet", 100),
                fileFactory.partitionFile("RL", "rl-compaction-1b.parquet", 100),
                fileFactory.partitionFile("RL", "rl-compaction-2a.parquet", 100),
                fileFactory.partitionFile("RL", "rl-compaction-2b.parquet", 100),
                fileFactory.partitionFile("root", "gc-1.parquet", 100),
                fileFactory.partitionFile("root", "gc-2.parquet", 100)));
        CompactionJob job1 = jobFactory.createCompactionJobWithFilenames("job-1", List.of("rl-compaction-1a.parquet", "rl-compaction-1b.parquet"), "RL");
        CompactionJob job2 = jobFactory.createCompactionJobWithFilenames("job-2", List.of("rl-compaction-2a.parquet", "rl-compaction-2b.parquet"), "RL");
        CompactionJob jobForGc = jobFactory.createCompactionJobWithFilenames("job-gc", List.of("gc-1.parquet", "gc-2.parquet"), "root");
        setTime(Instant.parse("2022-08-22T14:21:00Z"));
        update(stateStore).assignJobIds(assignJobFiles(List.of(job1, job2, jobForGc)));
        setTime(Instant.parse("2022-08-22T14:22:00Z"));
        update(stateStore).atomicallyReplaceFileReferencesWithNewOnes("job-gc", List.of("gc-1.parquet", "gc-2.parquet"),
                fileFactory.partitionFile("root", "root-output.parquet", 100));
        setJobTrackerData(
                compactionJobCreated(job1, Instant.parse("2022-08-22T14:21:10Z"),
                        jobRunOnTask("test-task",
                                compactionStartedStatus(Instant.parse("2022-08-22T14:21:11Z")),
                                compactionFinishedStatus(summary(Instant.parse("2022-08-22T14:21:11Z"), Duration.ofSeconds(10), 200, 200)))),
                compactionJobCreated(job2, Instant.parse("2022-08-22T14:21:10Z"),
                        jobRunOnTask("test-task",
                                compactionStartedStatus(Instant.parse("2022-08-22T14:21:23Z")),
                                compactionFinishedStatus(summary(Instant.parse("2022-08-22T14:21:23Z"), Duration.ofSeconds(10), 200, 200)))),
                compactionJobCreated(jobForGc, Instant.parse("2022-08-22T14:21:10Z"),
                        jobRunOnTask("test-task",
                                compactionStartedStatus(Instant.parse("2022-08-22T14:21:35Z")),
                                compactionFinishedStatus(summary(Instant.parse("2022-08-22T14:21:35Z"), Duration.ofSeconds(10), 200, 200)),
                                compactionCommittedStatus(Instant.parse("2022-08-22T14:22:00Z")))));
    }

    @Test
    void shouldShowStatsOnly() {
        // When / Then
        Approvals.verify(standardReportString(FileStatusDetailLevel.STATS_ONLY),
                new Options().forFile().withBaseName("detailLevelStatsOnly"));
    }

    @Test
    void shouldShowReferences() {
        // When / Then
        Approvals.verify(standardReportString(FileStatusDetailLevel.REFERENCES),
                new Options().forFile().withBaseName("detailLevelReferences"));
    }

    @Test
    void shouldShowCompactions() {
        // When / Then
        Approvals.verify(standardReportString(FileStatusDetailLevel.COMPACTION_JOBS),
                new Options().forFile().withBaseName("detailLevelCompactions"));
    }

    @Test
    void shouldBuildJson() {
        // When / Then
        Approvals.verify(verboseReportString(JsonFileStatusReporter::new),
                new Options().forFile().withName("detailLevelJson", ".json"));
    }

}
