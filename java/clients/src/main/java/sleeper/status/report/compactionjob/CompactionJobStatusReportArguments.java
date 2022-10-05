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
package sleeper.status.report.compactionjob;

import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class CompactionJobStatusReportArguments {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardCompactionJobStatusReporter());
        REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private static final Map<String, QueryType> QUERY_TYPES = new HashMap<>();

    static {
        QUERY_TYPES.put("-d", QueryType.DETAILED);
        QUERY_TYPES.put("-r", QueryType.RANGE);
        QUERY_TYPES.put("-u", QueryType.UNFINISHED);
    }

    private final String instanceId;
    private final String tableName;
    private final CompactionJobStatusReporter reporter;
    private final QueryType queryType;
    private final String queryParameters;

    public CompactionJobStatusReportArguments(String[] args) {
        if (args.length < 2 || args.length > 5) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        this.instanceId = args[0];
        this.tableName = args[1];
        this.reporter = getReporter(args, 2);
        this.queryType = getQueryType(args, 3);
        this.queryParameters = optionalArgument(args, 4).orElse(null);
        if (this.queryParameters == null && isParametersRequired(this.queryType)) {
            throw new IllegalArgumentException("No parameters provided for query type " + this.queryType);
        }
    }

    public static void printUsage() {
        System.out.println("Usage: <instance id> <table name> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
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

    public String getQueryParameters() {
        return queryParameters;
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
                .orElse(null);
    }

    private static QueryType readQueryType(String queryTypeStr) {
        if (!QUERY_TYPES.containsKey(queryTypeStr)) {
            throw new IllegalArgumentException("Invalid query type " + queryTypeStr + ". Valid query types are -d (Detailed), -r (Range), -u (Unfinished)");
        }
        return QUERY_TYPES.get(queryTypeStr);
    }

    private static boolean isParametersRequired(QueryType queryType) {
        return queryType != null && !(queryType.equals(QueryType.UNFINISHED) || queryType.equals(QueryType.ALL));
    }

    private static Optional<String> optionalArgument(String[] args, int index) {
        if (args.length > index) {
            return Optional.of(args[index]);
        } else {
            return Optional.empty();
        }
    }

}
