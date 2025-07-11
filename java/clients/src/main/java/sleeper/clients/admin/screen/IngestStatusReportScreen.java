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

package sleeper.clients.admin.screen;

import sleeper.clients.admin.AdminClientTrackerFactory;
import sleeper.clients.admin.TableSelectHelper;
import sleeper.clients.admin.properties.AdminClientPropertiesStore;
import sleeper.clients.report.IngestJobStatusReport;
import sleeper.clients.report.IngestTaskStatusReport;
import sleeper.clients.report.TableNamesReport;
import sleeper.clients.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.report.ingest.task.IngestTaskQuery;
import sleeper.clients.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.clients.report.job.query.JobQuery;
import sleeper.clients.util.console.ConsoleHelper;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.menu.MenuOption;
import sleeper.common.task.QueueMessageCount;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.table.TableStatus;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadInstanceProperties;
import static sleeper.clients.admin.screen.JobStatusScreenHelper.promptForJobId;
import static sleeper.clients.admin.screen.JobStatusScreenHelper.promptForRange;
import static sleeper.core.properties.instance.IngestProperty.INGEST_TRACKER_ENABLED;

public class IngestStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminClientPropertiesStore store;
    private final AdminClientTrackerFactory trackers;
    private final QueueMessageCount.Client queueClient;
    private final TableSelectHelper tableSelectHelper;
    private final Function<InstanceProperties, Map<String, Integer>> getStepCount;

    public IngestStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store,
            AdminClientTrackerFactory trackers, QueueMessageCount.Client queueClient,
            Function<InstanceProperties, Map<String, Integer>> getStepCount, TableNamesReport tableNamesReport) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.trackers = trackers;
        this.queueClient = queueClient;
        this.tableSelectHelper = new TableSelectHelper(out, in, store, tableNamesReport);
        this.getStepCount = getStepCount;
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> propertiesOpt = tryLoadInstanceProperties(out, in, store, instanceId);
        if (propertiesOpt.isPresent()) {
            InstanceProperties properties = propertiesOpt.get();
            if (!properties.getBoolean(INGEST_TRACKER_ENABLED)) {
                out.println("");
                out.println("Ingest tracker not enabled. Please enable in instance properties to access this screen");
                confirmReturnToMainScreen(out, in);
            } else {
                out.clearScreen("");
                consoleHelper.chooseOptionUntilValid("Which ingest report would you like to run",
                        new MenuOption("Ingest Job Status Report", () -> chooseArgsForIngestJobStatusReport(properties)),
                        new MenuOption("Ingest Task Status Report", () -> chooseArgsForIngestTaskStatusReport(properties))).run();
            }
        }
    }

    private void chooseArgsForIngestJobStatusReport(InstanceProperties properties) throws InterruptedException {
        Optional<TableProperties> tableOpt = tableSelectHelper.chooseTableOrReturnToMain(properties);
        if (tableOpt.isPresent()) {
            TableStatus table = tableOpt.get().getStatus();
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All", () -> runIngestJobStatusReport(properties, table, JobQuery.Type.ALL)),
                    new MenuOption("Unfinished", () -> runIngestJobStatusReport(properties, table, JobQuery.Type.UNFINISHED)),
                    new MenuOption("Detailed", () -> runIngestJobStatusReport(properties, table, JobQuery.Type.DETAILED, promptForJobId(in))),
                    new MenuOption("Range", () -> runIngestJobStatusReport(properties, table, JobQuery.Type.RANGE, promptForRange(in))),
                    new MenuOption("Rejected", () -> runIngestJobStatusReport(properties, table, JobQuery.Type.REJECTED))).run();
        }
    }

    private void chooseArgsForIngestTaskStatusReport(InstanceProperties properties) throws InterruptedException {
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                new MenuOption("All", () -> runIngestTaskStatusReport(properties, IngestTaskQuery.ALL)),
                new MenuOption("Unfinished", () -> runIngestTaskStatusReport(properties, IngestTaskQuery.UNFINISHED))).run();
    }

    private void runIngestJobStatusReport(InstanceProperties properties, TableStatus table,
            JobQuery.Type queryType) {
        runIngestJobStatusReport(properties, table, queryType, "");
    }

    private void runIngestJobStatusReport(InstanceProperties properties, TableStatus table,
            JobQuery.Type queryType, String queryParameters) {
        new IngestJobStatusReport(trackers.loadIngestJobTracker(properties), table, queryType, queryParameters,
                new StandardIngestJobStatusReporter(out.printStream()),
                queueClient, properties, getStepCount.apply(properties)).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runIngestTaskStatusReport(InstanceProperties properties, IngestTaskQuery queryType) {
        new IngestTaskStatusReport(trackers.loadIngestTaskTracker(properties),
                new StandardIngestTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}
