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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static sleeper.clients.util.ClientUtils.optionalArgument;

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
        try {
            if (args.length < 1 || args.length > 2) {
                throw new IllegalArgumentException("Wrong number of arguments");
            }
            String instanceId = args[0];
            ListTablesReporter reporter = getReporter(args, 1);

            try (SleeperClient client = SleeperClient.builder().instanceId(instanceId).build()) {
                new ListTablesReport(client, reporter).run();
            }
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            printUsage();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("" +
                "Usage: <instance-id> <optional-report-type-standard-or-json>");
    }

    private static ListTablesReporter getReporter(String[] args, int index) {
        String reporterType = optionalArgument(args, index)
                .map(str -> str.toUpperCase(Locale.ROOT))
                .orElse(DEFAULT_REPORTER);
        if (!REPORTERS.containsKey(reporterType)) {
            throw new IllegalArgumentException("Output type not supported: " + reporterType);
        }
        return REPORTERS.get(reporterType);
    }
}
