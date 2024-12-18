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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.compaction.task.CompactionTaskStatus;
import sleeper.core.tracker.ingest.job.IngestJobStatus;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.reporting.CompactionReportsDriver;
import sleeper.systemtest.dsl.reporting.IngestReportsDriver;
import sleeper.systemtest.dsl.reporting.PartitionReportDriver;
import sleeper.systemtest.dsl.reporting.ReportingContext;
import sleeper.systemtest.dsl.reporting.SystemTestReport;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class InMemoryReports {

    private final InMemoryIngestByQueue ingest;
    private final InMemoryCompaction compaction;
    private SystemTestReport compactionReport;
    private SystemTestReport ingestTasksReport;
    private SystemTestReport ingestJobsReport;
    private SystemTestReport partitionsReport;

    public InMemoryReports(InMemoryIngestByQueue ingest, InMemoryCompaction compaction) {
        this.ingest = ingest;
        this.compaction = compaction;
    }

    public IngestReportsDriver ingest(SystemTestInstanceContext instance) {
        return new Ingest(instance);
    }

    public CompactionReportsDriver compaction(SystemTestInstanceContext instance) {
        return new Compaction(instance);
    }

    public PartitionReportDriver partitions(SystemTestInstanceContext instance) {
        return new Partitions(instance);
    }

    public void setIngestTasksReport(SystemTestReport ingestTasksReport) {
        this.ingestTasksReport = ingestTasksReport;
    }

    public void setIngestJobsReport(SystemTestReport ingestJobsReport) {
        this.ingestJobsReport = ingestJobsReport;
    }

    public void setCompactionReport(SystemTestReport compactionReport) {
        this.compactionReport = compactionReport;
    }

    public void setPartitionsReport(SystemTestReport partitionsReport) {
        this.partitionsReport = partitionsReport;
    }

    private class Ingest implements IngestReportsDriver {

        private final SystemTestInstanceContext instance;

        private Ingest(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public SystemTestReport tasksAndJobsReport() {
            return SystemTestReport.join(tasksReport(), jobsReport());
        }

        @Override
        public SystemTestReport tasksReport() {
            return Objects.requireNonNull(ingestTasksReport, "Ingest tasks report is not set");
        }

        @Override
        public SystemTestReport jobsReport() {
            return Objects.requireNonNull(ingestJobsReport, "Ingest jobs report is not set");
        }

        @Override
        public List<IngestJobStatus> jobs(ReportingContext reportingContext) {
            return ingest.jobStore().getJobsInTimePeriod(
                    instance.getTableStatus().getTableUniqueId(),
                    reportingContext.getRecordingStartTime(), Instant.MAX);
        }
    }

    private class Compaction implements CompactionReportsDriver {

        private final SystemTestInstanceContext instance;

        private Compaction(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public SystemTestReport tasksAndJobsReport() {
            return Objects.requireNonNull(compactionReport, "Compaction report is not set");
        }

        @Override
        public List<CompactionJobStatus> jobs(ReportingContext reportingContext) {
            return compaction.jobTracker().getJobsInTimePeriod(
                    instance.getTableStatus().getTableUniqueId(),
                    reportingContext.getRecordingStartTime(), Instant.MAX);
        }

        @Override
        public List<CompactionTaskStatus> tasks(ReportingContext reportingContext) {
            return compaction.taskTracker().getTasksInTimePeriod(
                    reportingContext.getRecordingStartTime(), Instant.MAX);
        }
    }

    private class Partitions implements PartitionReportDriver {

        private final SystemTestInstanceContext instance;

        private Partitions(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public SystemTestReport statusReport() {
            return Objects.requireNonNull(partitionsReport, "Partitions report is not set");
        }
    }

}
