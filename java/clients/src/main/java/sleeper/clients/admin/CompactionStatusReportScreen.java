/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.clients.admin;

import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.status.report.CompactionJobStatusReport;
import sleeper.clients.status.report.CompactionTaskStatusReport;
import sleeper.clients.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.console.ConsoleHelper;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.menu.MenuOption;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableStatus;

import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadInstanceProperties;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForJobId;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForRange;
import static sleeper.core.properties.instance.CompactionProperty.COMPACTION_TRACKER_ENABLED;

public class CompactionStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminClientPropertiesStore store;
    private final AdminClientTrackerFactory trackers;
    private final TableSelectHelper tableSelectHelper;

    public CompactionStatusReportScreen(
            ConsoleOutput out, ConsoleInput in,
            AdminClientPropertiesStore store, AdminClientTrackerFactory trackers) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.trackers = trackers;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> propertiesOpt = tryLoadInstanceProperties(out, in, store, instanceId);
        if (propertiesOpt.isPresent()) {
            InstanceProperties properties = propertiesOpt.get();
            if (!properties.getBoolean(COMPACTION_TRACKER_ENABLED)) {
                out.println("");
                out.println("Compaction tracker not enabled. Please enable in instance properties to access this screen");
                confirmReturnToMainScreen(out, in);
            } else {
                out.clearScreen("");
                consoleHelper.chooseOptionUntilValid("Which compaction report would you like to run",
                        new MenuOption("Compaction Job Status Report", () -> chooseArgsForCompactionJobStatusReport(properties)),
                        new MenuOption("Compaction Task Status Report", () -> chooseArgsForCompactionTaskStatusReport(properties))).run();
            }
        }
    }

    private void chooseArgsForCompactionJobStatusReport(InstanceProperties properties) throws InterruptedException {
        Optional<TableProperties> tableOpt = tableSelectHelper.chooseTableOrReturnToMain(properties);
        if (tableOpt.isPresent()) {
            TableStatus table = tableOpt.get().getStatus();
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All", () -> runCompactionJobStatusReport(properties, table, JobQuery.Type.ALL)),
                    new MenuOption("Unfinished", () -> runCompactionJobStatusReport(properties, table, JobQuery.Type.UNFINISHED)),
                    new MenuOption("Detailed", () -> runCompactionJobStatusReport(properties, table, JobQuery.Type.DETAILED, promptForJobId(in))),
                    new MenuOption("Range", () -> runCompactionJobStatusReport(properties, table, JobQuery.Type.RANGE, promptForRange(in)))).run();
        }
    }

    private void chooseArgsForCompactionTaskStatusReport(InstanceProperties properties) throws InterruptedException {
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use?",
                new MenuOption("All", () -> runCompactionTaskStatusReport(properties, CompactionTaskQuery.ALL)),
                new MenuOption("Unfinished", () -> runCompactionTaskStatusReport(properties, CompactionTaskQuery.UNFINISHED))).run();
    }

    private void runCompactionJobStatusReport(InstanceProperties properties, TableStatus table, JobQuery.Type queryType) {
        runCompactionJobStatusReport(properties, table, queryType, "");
    }

    private void runCompactionJobStatusReport(InstanceProperties properties, TableStatus table, JobQuery.Type queryType, String queryParameters) {
        new CompactionJobStatusReport(trackers.loadCompactionJobTracker(properties),
                new StandardCompactionJobStatusReporter(out.printStream()), table, queryType, queryParameters).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runCompactionTaskStatusReport(InstanceProperties properties, CompactionTaskQuery queryType) {
        new CompactionTaskStatusReport(trackers.loadCompactionTaskTracker(properties),
                new StandardCompactionTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}
