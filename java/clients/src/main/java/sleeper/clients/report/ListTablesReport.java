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

package sleeper.clients.report;

import sleeper.clients.api.SleeperClient;
import sleeper.clients.report.tables.JsonListTablesReporter;
import sleeper.clients.report.tables.ListTablesReporter;
import sleeper.clients.report.tables.StandardListTablesReporter;
import sleeper.core.table.TableStatus;
import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandLineUsage;
import sleeper.core.util.cli.CommandOption;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Lists all tables in a Sleeper instance with ID, either in standard or JSON format.
 */
public class ListTablesReport {
    private static final String DEFAULT_REPORTER = "STANDARD";
    private static final Map<String, ListTablesReporter> REPORTERS = new HashMap<>();

    static {
        REPORTERS.put(DEFAULT_REPORTER, new StandardListTablesReporter());
        REPORTERS.put("JSON", new JsonListTablesReporter());
    }

    public static final CommandLineUsage USAGE = CommandLineUsage.builder()
            .positionalArguments(List.of("instance-id"))
            .options(List.of(CommandOption.longOption("report-type")))
            .helpSummary("" +
                    "Creates a report listing all the tables within a Sleeper instance.\n" +
                    "\n" +
                    "--report-type <type>\n" +
                    "Output format. One of STANDARD, JSON. Defaults to STANDARD.")
            .build();

    private final SleeperClient client;
    private final ListTablesReporter reporter;

    public ListTablesReport(SleeperClient client, ListTablesReporter reporter) {
        this.client = client;
        this.reporter = reporter;
    }

    /**
     * Creates a report.
     */
    public void run() {
        reporter.report(client.streamAllTables().sorted(Comparator.comparing(TableStatus::getTableName)));
    }

    public static void main(String[] args) {
        Arguments arguments = CommandArguments.parseAndValidateOrExit(USAGE, args, ListTablesReport::readArguments);

        try (SleeperClient client = SleeperClient.builder().instanceId(arguments.instanceId()).build()) {
            new ListTablesReport(client, REPORTERS.get(arguments.reportType())).run();
        }
    }

    /**
     * Reads the arguments from the command line.
     *
     * @param  arguments the parsed command line arguments
     * @return           the arguments
     */
    public static Arguments readArguments(CommandArguments arguments) {
        return new Arguments(
                arguments.getString("instance-id"),
                arguments.getOptionalString("report-type")
                        .map(s -> s.toUpperCase(Locale.ROOT))
                        .orElse(DEFAULT_REPORTER));
    }

    /**
     * Holds the arguments for the list tables report command.
     * Arguments
     *
     * @param instanceId the Sleeper instance ID
     * @param reportType the output format, either STANDARD or JSON
     */
    public record Arguments(String instanceId, String reportType) {
        public Arguments {
            if (!REPORTERS.containsKey(reportType)) {
                throw new CommandArgumentsException("Report type not supported: " + reportType + ". Valid types: " + String.join(", ", REPORTERS.keySet()));
            }
        }
    }
}
