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

import sleeper.clients.report.job.query.RangeJobsQuery;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class JobTrackerReportOptions {

    private static final CommandOption ALL = CommandOption
            .withLongName("all").shortName('a')
            .helpText("Reports on all jobs.").build();
    private static final CommandOption DETAILED = CommandOption
            .withLongName("detailed").shortName('d').numArgs(NumArgs.ONE)
            .helpText("Reports in detail on the jobs with the given IDs. Separate several IDs with commas.")
            .argsHelpText("<job-id>[,<more-ids>]")
            .build();
    private static final CommandOption RANGE = CommandOption
            .withLongName("range").shortName('r')
            .helpText("Reports on all jobs in a time period. Defaults to the last 4 hours, " +
                    "or set the period with --start-time and --end-time.")
            .build();
    private static final CommandOption UNFINISHED = CommandOption
            .withLongName("unfinished").shortName('u')
            .helpText("Reports on all unfinished jobs.")
            .build();
    private static final CommandOption START_TIME = CommandOption
            .withLongName("start-time").numArgs(NumArgs.ONE)
            .helpText("Start of the period to report on, in the format " + RangeJobsQuery.DATE_FORMAT + ". " +
                    "Must be set together with --end-time, and only applies to the --range query type.")
            .argsHelpText("<" + RangeJobsQuery.DATE_FORMAT + ">")
            .build();
    private static final CommandOption END_TIME = CommandOption
            .withLongName("end-time").numArgs(NumArgs.ONE)
            .helpText("End of the period to report on, in the format " + RangeJobsQuery.DATE_FORMAT + ". " +
                    "Must be set together with --start-time, and only applies to the --range query type.")
            .argsHelpText("<" + RangeJobsQuery.DATE_FORMAT + ">")
            .build();
    // Note that compaction jobs are never rejected, so this only applies to ingest.
    private static final CommandOption REJECTED = CommandOption
            .withLongName("rejected").shortName('n')
            .helpText("Reports on all rejected jobs.")
            .build();

    public static List<CommandOption> forIngest(CommandOption outputOption) {
        return Stream.of(ALL, DETAILED, END_TIME, RANGE, REJECTED, START_TIME, UNFINISHED, outputOption)
                .sorted(Comparator.comparing(CommandOption::longName))
                .toList();
    }

}
