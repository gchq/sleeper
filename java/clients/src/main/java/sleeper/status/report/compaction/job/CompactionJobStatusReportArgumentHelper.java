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
package sleeper.status.report.compaction.job;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.util.ClientUtils.optionalArgument;

public class CompactionJobStatusReportArgumentHelper {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardCompactionJobStatusReporter());
        REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    private CompactionJobStatusReportArgumentHelper() {
    }

    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance id> <table name> <report_type_standard_or_json> <optional_query_type> <optional_query_parameters> \n" +
                "Query types are:\n" +
                "-a (Return all jobs)\n" +
                "-d (Detailed, provide a jobId)\n" +
                "-r (Provide startRange and endRange separated by commas in format yyyyMMddhhmmss)\n" +
                "-u (Unfinished jobs)");
    }

    public static CompactionJobStatusReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
