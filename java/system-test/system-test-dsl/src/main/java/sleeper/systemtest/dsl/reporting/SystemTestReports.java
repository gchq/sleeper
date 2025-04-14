/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.systemtest.dsl.reporting;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.util.TestContext;

import java.util.ArrayList;
import java.util.List;

public class SystemTestReports {

    private final ReportingContext context;
    private final List<SystemTestReport> reports;

    private SystemTestReports(Builder builder) {
        context = builder.context;
        reports = builder.reports;
    }

    public static Builder builder(ReportingContext context) {
        return new Builder(context);
    }

    public static SystemTestBuilder builder(SystemTestContext context) {
        return new SystemTestBuilder(context);
    }

    public void print(TestContext testContext) {
        context.print(testContext, (out, startTime) -> reports.forEach(report -> report.print(out, startTime)));
    }

    public static class Builder {
        private final ReportingContext context;
        private final List<SystemTestReport> reports = new ArrayList<>();

        private Builder(ReportingContext context) {
            this.context = context;
        }

        public Builder report(SystemTestReport report) {
            reports.add(report);
            return this;
        }

        public SystemTestReports build() {
            return new SystemTestReports(this);
        }
    }

    public static class SystemTestBuilder extends Builder {

        private final PartitionReportDriver partitionDriver;
        private final IngestReportsDriver ingestDriver;
        private final CompactionReportsDriver compactionDriver;

        private SystemTestBuilder(SystemTestContext context) {
            super(context.reporting());
            SystemTestDrivers drivers = context.instance().adminDrivers();
            this.partitionDriver = drivers.partitionReports(context);
            this.ingestDriver = drivers.ingestReports(context);
            this.compactionDriver = drivers.compactionReports(context);
        }

        public Builder ingestTasksAndJobs() {
            return report(ingestDriver.tasksAndJobsReport());
        }

        public Builder ingestJobs() {
            return report(ingestDriver.jobsReport());
        }

        public Builder compactionTasksAndJobs() {
            return report(compactionDriver.tasksAndJobsReport());
        }

        public Builder partitionStatus() {
            return report(partitionDriver.statusReport());
        }
    }
}
