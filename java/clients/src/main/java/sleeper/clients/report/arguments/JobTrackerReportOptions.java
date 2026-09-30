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
package sleeper.clients.report.arguments;

import sleeper.clients.report.ingest.job.IngestJobStatusReporter;
import sleeper.clients.report.ingest.job.JsonIngestJobStatusReporter;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.report.job.query.JobQueryPrompt;
import sleeper.clients.report.job.query.JobQueryType;
import sleeper.clients.report.job.query.RejectedJobsQuery;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandOption;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Handling of command line options for reports generated from job trackers. Shared between ingest and compaction job
 * reporting.
 */
public class JobTrackerReportOptions {

    private JobTrackerReportOptions() {
    }

    public static final ReportTypeArgument<IngestJobStatusReporter> INGEST_REPORT_TYPE = ReportTypeArgument
            .<IngestJobStatusReporter>withDefault("STANDARD", new StandardIngestJobStatusReporter())
            .addReporter("JSON", new JsonIngestJobStatusReporter())
            .build();

    public static final List<CommandOption> INGEST_OPTIONS = Stream.concat(
            JobQueryType.INGEST_OPTIONS.stream().flatMap(type -> type.options().stream()),
            Stream.of(INGEST_REPORT_TYPE.option()))
            .sorted(Comparator.comparing(CommandOption::longName))
            .toList();

    /**
     * Reads the ingest job tracker query requested from the command line.
     *
     * @param  arguments the command line arguments
     * @param  clock     the clock to get the current time
     * @param  input     the console to prompt the user for further input
     * @return           the query
     */
    public static JobQuery readIngestJobQuery(CommandArguments arguments, Clock clock, ConsoleInput input) {
        return JobQuery.forIngest(arguments, clock)
                .orElseGet(() -> JobQueryPrompt.from(clock, input, Map.of("n", new RejectedJobsQuery())));
    }

}
