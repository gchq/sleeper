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

package sleeper.clients.admin;

import sleeper.clients.status.report.IngestJobStatusReport;
import sleeper.clients.status.report.IngestTaskStatusReport;
import sleeper.clients.status.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.clients.status.report.ingest.task.IngestTaskQuery;
import sleeper.clients.status.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.util.console.ConsoleHelper;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.menu.MenuOption;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.job.common.QueueMessageCount;

import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.AdminCommonPrompts.tryLoadInstanceProperties;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForJobId;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForRange;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_STATUS_STORE_ENABLED;

public class IngestStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminClientPropertiesStore store;
    private final AdminClientStatusStoreFactory statusStores;
    private final QueueMessageCount.Client queueClient;
    private final TableSelectHelper tableSelectHelper;

    public IngestStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store,
                                    AdminClientStatusStoreFactory statusStores,
                                    QueueMessageCount.Client queueClient) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.statusStores = statusStores;
        this.queueClient = queueClient;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        Optional<InstanceProperties> propertiesOpt = tryLoadInstanceProperties(out, in, store, instanceId);
        if (propertiesOpt.isPresent()) {
            InstanceProperties properties = propertiesOpt.get();
            if (!properties.getBoolean(INGEST_STATUS_STORE_ENABLED)) {
                out.println("");
                out.println("Ingest status store not enabled. Please enable in instance properties to access this screen");
                confirmReturnToMainScreen(out, in);
            } else {
                out.clearScreen("");
                consoleHelper.chooseOptionUntilValid("Which ingest report would you like to run",
                        new MenuOption("Ingest Job Status Report", () ->
                                chooseArgsForIngestJobStatusReport(properties)),
                        new MenuOption("Ingest Task Status Report", () ->
                                chooseArgsForIngestTaskStatusReport(properties))
                ).run();
            }
        }
    }

    private void chooseArgsForIngestJobStatusReport(InstanceProperties properties) throws InterruptedException {
        Optional<TableProperties> tableOpt = tableSelectHelper.chooseTableOrReturnToMain(properties);
        if (tableOpt.isPresent()) {
            String tableName = tableOpt.get().get(TableProperty.TABLE_NAME);
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All", () ->
                            runIngestJobStatusReport(properties, tableName, JobQuery.Type.ALL)),
                    new MenuOption("Unfinished", () ->
                            runIngestJobStatusReport(properties, tableName, JobQuery.Type.UNFINISHED)),
                    new MenuOption("Detailed", () ->
                            runIngestJobStatusReport(properties, tableName, JobQuery.Type.DETAILED, promptForJobId(in))),
                    new MenuOption("Range", () ->
                            runIngestJobStatusReport(properties, tableName, JobQuery.Type.RANGE, promptForRange(in)))
            ).run();
        }
    }

    private void chooseArgsForIngestTaskStatusReport(InstanceProperties properties) throws InterruptedException {
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                new MenuOption("All", () ->
                        runIngestTaskStatusReport(properties, IngestTaskQuery.ALL)),
                new MenuOption("Unfinished", () ->
                        runIngestTaskStatusReport(properties, IngestTaskQuery.UNFINISHED))
        ).run();
    }

    private void runIngestJobStatusReport(InstanceProperties properties, String tableName,
                                          JobQuery.Type queryType) {
        runIngestJobStatusReport(properties, tableName, queryType, "");
    }

    private void runIngestJobStatusReport(InstanceProperties properties, String tableName,
                                          JobQuery.Type queryType, String queryParameters) {
        new IngestJobStatusReport(statusStores.loadIngestJobStatusStore(properties), tableName, queryType, queryParameters,
                new StandardIngestJobStatusReporter(out.printStream()),
                queueClient, properties).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runIngestTaskStatusReport(InstanceProperties properties, IngestTaskQuery queryType) {
        new IngestTaskStatusReport(statusStores.loadIngestTaskStatusStore(properties),
                new StandardIngestTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}
