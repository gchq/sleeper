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
package sleeper.clients.report.job.query;

import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A query to generate a detailed report on specific jobs, against a job tracker.
 */
public class DetailedJobsQuery implements JobQuery {

    public static final CommandOption COMMAND_OPTION = CommandOption
            .withLongName("detailed").shortName('d').numArgs(NumArgs.ONE)
            .helpText("Reports in detail on the jobs with the given IDs. Separate several IDs with commas.")
            .argsHelpText("<job-id>[,<more-ids>]")
            .build();

    private final List<String> jobIds;

    public DetailedJobsQuery(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    /**
     * Creates a parser for this query type.
     *
     * @return this parser
     */
    public static JobQueryTypeParser parser() {
        return new JobQueryTypeParser(COMMAND_OPTION, JobQueryType.DETAILED,
                (parameters, timeSupplier) -> fromParameters(parameters),
                (arguments, timeSupplier) -> fromCommandLine(arguments.getString("detailed")));
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobTracker tracker, String tableId) {
        return run(tracker::getJob);
    }

    @Override
    public List<IngestJobStatus> run(IngestJobTracker tracker, String tableId) {
        return run(tracker::getJob);
    }

    @Override
    public JobQueryType getType() {
        return JobQueryType.DETAILED;
    }

    private <T> List<T> run(Function<String, Optional<T>> getJob) {
        return jobIds.stream()
                .map(getJob)
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Reads a command line parameter that sets which jobs should be included in a detailed report.
     *
     * @param  queryParameters the job IDs separated by commas
     * @return                 the query for a detailed report on those jobs
     */
    public static JobQuery fromParameters(String queryParameters) {
        if ("".equals(queryParameters)) {
            return null;
        }
        return new DetailedJobsQuery(Arrays.asList(queryParameters.split(",")));
    }

    private static JobQuery fromCommandLine(String jobIds) {
        if ("".equals(jobIds)) {
            throw new CommandArgumentsException("Expected a value for option: detailed");
        }
        return new DetailedJobsQuery(Arrays.asList(jobIds.split(",")));
    }
}
