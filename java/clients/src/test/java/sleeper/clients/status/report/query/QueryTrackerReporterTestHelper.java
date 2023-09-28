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

package sleeper.clients.status.report.query;

import sleeper.clients.status.report.compaction.job.CompactionJobStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.testutil.ToStringPrintStream;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.query.tracker.TrackedQuery;

import java.io.PrintStream;
import java.util.List;
import java.util.function.Function;

public class QueryTrackerReporterTestHelper {
    public static String getStandardReport(TrackerQuery query, List<TrackedQuery> trackedQueries) {
        ToStringPrintStream output = new ToStringPrintStream();
        new StandardQueryTrackerReporter(output.getPrintStream())
                .report(query, trackedQueries);
        return output.toString();
    }

    public String verboseReportString(Function<PrintStream, CompactionJobStatusReporter> getReporter, List<CompactionJobStatus> statusList,
                                      JobQuery.Type queryType) {
        ToStringPrintStream out = new ToStringPrintStream();
        getReporter.apply(out.getPrintStream())
                .report(statusList, queryType);
        return out.toString();
    }
}
