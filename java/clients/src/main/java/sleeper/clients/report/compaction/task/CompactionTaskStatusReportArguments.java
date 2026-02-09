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
package sleeper.clients.report.compaction.task;

import java.io.PrintStream;

import static sleeper.clients.util.ClientUtils.optionalArgument;

/**
 * Command line arguments to create a report on compaction tasks.
 */
public class CompactionTaskStatusReportArguments {

    private final String instanceId;
    private final CompactionTaskStatusReporter reporter;
    private final CompactionTaskQuery query;

    private CompactionTaskStatusReportArguments(
            String instanceId, CompactionTaskStatusReporter reporter, CompactionTaskQuery query) {
        this.instanceId = instanceId;
        this.reporter = reporter;
        this.query = query;
    }

    /**
     * Reads command line arguments to create a report on compaction tasks.
     *
     * @param  args                     the arguments
     * @return                          the parsed arguments
     * @throws IllegalArgumentException if the arguments could not be read
     */
    public static CompactionTaskStatusReportArguments fromArgs(String... args) {
        if (args.length < 1 || args.length > 3) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        CompactionTaskStatusReporter reporter = optionalArgument(args, 1)
                .map(type -> CompactionTaskStatusReporter.from(type, System.out))
                .orElseGet(() -> new StandardCompactionTaskStatusReporter(System.out));
        CompactionTaskQuery query = optionalArgument(args, 2)
                .map(CompactionTaskQuery::from)
                .orElse(CompactionTaskQuery.ALL);
        return new CompactionTaskStatusReportArguments(args[0], reporter, query);
    }

    /**
     * Prints the usage message to explain what arguments can be given.
     *
     * @param out the output to write to
     */
    public static void printUsage(PrintStream out) {
        out.println("Usage: <instance-id> <report-type-standard-or-json> <optional-query-type>\n" +
                "Query types are:\n" +
                "-a (Return all tasks)\n" +
                "-u (Unfinished tasks)");
    }

    public String getInstanceId() {
        return instanceId;
    }

    public CompactionTaskStatusReporter getReporter() {
        return reporter;
    }

    public CompactionTaskQuery getQuery() {
        return query;
    }
}
