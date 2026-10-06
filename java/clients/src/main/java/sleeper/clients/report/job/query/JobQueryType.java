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

import sleeper.core.util.cli.CommandOption;

import java.util.List;
import java.util.Objects;

/**
 * The type of a query for jobs to include in a report.
 */
public enum JobQueryType {
    PROMPT(null),
    ALL(AllJobsQuery.parser()),
    DETAILED(DetailedJobsQuery.parser()),
    RANGE(RangeJobsQuery.parser()),
    UNFINISHED(UnfinishedJobsQuery.parser()),
    REJECTED(RejectedJobsQuery.parser());

    public static final List<JobQueryType> INGEST_OPTIONS = List.of(ALL, DETAILED, RANGE, UNFINISHED, REJECTED);

    private final JobQueryTypeParser parser;

    JobQueryType(JobQueryTypeParser parser) {
        this.parser = parser;
    }

    /**
     * Retrieves a parser to read a job query if it is of this type.
     *
     * @return the parser
     */
    public JobQueryTypeParser parser() {
        return Objects.requireNonNull(parser, "Query type has no parser: " + this);
    }

    /**
     * Retrieves the command line options that are only used by this job query type.
     *
     * @return the options
     */
    public List<CommandOption> options() {
        return parser().options();
    }
}
