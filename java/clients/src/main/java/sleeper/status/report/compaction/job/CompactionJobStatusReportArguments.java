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
package sleeper.status.report.compaction.job;

import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.status.report.compaction.job.CompactionJobStatusReporter.QueryType;

import java.io.PrintStream;
import java.time.Clock;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static sleeper.ClientUtils.optionalArgument;

public class CompactionJobStatusReportArguments {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardCompactionJobStatusReporter());
        REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private static final Map<String, QueryType> QUERY_TYPES = new HashMap<>();

    static {
        QUERY_TYPES.put("-a", QueryType.ALL);
        QUERY_TYPES.put("-d", QueryType.DETAILED);
        QUERY_TYPES.put("-r", QueryType.RANGE);
        QUERY_TYPES.put("-u", QueryType.UNFINISHED);
    }

    private final String instanceId;
    private final String tableName;
    private final CompactionJobStatusReporter reporter;
    private final QueryType queryType;
    private final String queryParameters;

    private CompactionJobStatusReportArguments(Builder builder) {
        instanceId = Objects.requireNonNull(builder.instanceId, "instanceId must not be null");
        tableName = Objects.requireNonNull(builder.tableName, "tableName must not be null");
        reporter = Objects.requireNonNull(builder.reporter, "reporter must not be null");
        queryType = Objects.requireNonNull(builder.queryType, "queryType must not be null");
        queryParameters = builder.queryParameters;
        if (this.queryParameters == null && queryType.isParametersRequired()) {
            throw new IllegalArgumentException("No parameters provided for query type " + this.queryType);
        }
    }

    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance id> <table name> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    public static CompactionJobStatusReportArguments from(String... args) {
        if (args.length < 2 || args.length > 5) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        return builder()
                .instanceId(args[0])
                .tableName(args[1])
                .reporter(getReporter(args, 2))
                .queryType(getQueryType(args, 3))
                .queryParameters(optionalArgument(args, 4).orElse(null))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getTableName() {
        return tableName;
    }

    public CompactionJobStatusReporter getReporter() {
        return reporter;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public boolean isPromptForQuery() {
        return queryType == QueryType.PROMPT;
    }

    public CompactionJobQuery buildQuery(Clock clock, ConsoleInput input, ConsoleOutput output) {
        if (queryType == QueryType.PROMPT) {
            return CompactionJobQueryPrompt.from(tableName, input, output);
        }
        return CompactionJobQuery.from(tableName, queryType, queryParameters, clock);
    }

    private static CompactionJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }

    private static QueryType getQueryType(String[] args, int index) {
        return optionalArgument(args, index)
                .map(CompactionJobStatusReportArguments::readQueryType)
                .orElse(QueryType.PROMPT);
    }

    private static QueryType readQueryType(String queryTypeStr) {
        if (!QUERY_TYPES.containsKey(queryTypeStr)) {
            throw new IllegalArgumentException("Invalid query type " + queryTypeStr + ". Valid query types are -d (Detailed), -r (Range), -u (Unfinished)");
        }
        return QUERY_TYPES.get(queryTypeStr);
    }

    public static final class Builder {
        private String instanceId;
        private String tableName;
        private CompactionJobStatusReporter reporter;
        private QueryType queryType;
        private String queryParameters;

        private Builder() {
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder reporter(CompactionJobStatusReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public Builder queryType(QueryType queryType) {
            this.queryType = queryType;
            return this;
        }

        public Builder queryParameters(String queryParameters) {
            this.queryParameters = queryParameters;
            return this;
        }

        public CompactionJobStatusReportArguments build() {
            return new CompactionJobStatusReportArguments(this);
        }
    }
}
