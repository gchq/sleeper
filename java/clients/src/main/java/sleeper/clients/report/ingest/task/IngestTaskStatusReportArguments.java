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

package sleeper.clients.report.ingest.task;

import java.io.PrintStream;

import static sleeper.clients.util.ClientUtils.optionalArgument;

/**
 * Command line arguments to create a report on ingest tasks.
 */
public class IngestTaskStatusReportArguments {
    private final String instanceId;
    private final IngestTaskStatusReporter reporter;
    private final IngestTaskQuery query;

    private IngestTaskStatusReportArguments(String instanceId, IngestTaskStatusReporter reporter, IngestTaskQuery query) {
        this.instanceId = instanceId;
        this.reporter = reporter;
        this.query = query;
    }

    /**
     * Reads command line arguments to create a report on ingest tasks.
     *
     * @param  args                     the arguments
     * @return                          the parsed arguments
     * @throws IllegalArgumentException if the arguments could not be read
     */
    public static IngestTaskStatusReportArguments fromArgs(String... args) {
        if (args.length < 1 || args.length > 3) {
            throw new IllegalArgumentException("Wrong number of arguments");
        }
        IngestTaskStatusReporter reporter = optionalArgument(args, 1)
                .map(type -> IngestTaskStatusReporter.from(type, System.out))
                .orElseGet(() -> new StandardIngestTaskStatusReporter(System.out));
        IngestTaskQuery query = optionalArgument(args, 2)
                .map(IngestTaskQuery::from)
                .orElse(IngestTaskQuery.ALL);
        return new IngestTaskStatusReportArguments(args[0], reporter, query);
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

    public IngestTaskStatusReporter getReporter() {
        return reporter;
    }

    public IngestTaskQuery getQuery() {
        return query;
    }
}
