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

import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.MenuOption;
import sleeper.status.report.CompactionJobStatusReport;
import sleeper.status.report.compaction.job.CompactionJobStatusReportArguments;
import sleeper.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.status.report.job.query.JobQuery;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class CompactionStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;
    private final AdminConfigStore store;
    private final TableSelectHelper tableSelectHelper;

    public CompactionStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
        this.store = store;
        this.tableSelectHelper = new TableSelectHelper(out, in);
    }

    public void chooseArgsAndPrint(String instanceId) {
        out.clearScreen("");
        chooseOptionUntilValid("Which compaction report would you like to run",
                new MenuOption("Compaction Job Status Report", () ->
                        chooseArgsForCompactionJobStatusReport(instanceId)),
                new MenuOption("Compaction Task Status Report", () -> {
                })).run();
    }

    private void chooseArgsForCompactionJobStatusReport(String instanceId) {
        String tableName = tableSelectHelper.chooseTable().getEntered();
        chooseOptionUntilValid("Which query type would you like to use",
                new MenuOption("All", () ->
                        runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.ALL)),
                new MenuOption("Unfinished jobs", () ->
                        runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.UNFINISHED))).run();
    }

    private CompactionJobStatusReportArguments.Builder argsBuilder(String instanceId, String tableName, JobQuery.Type queryType) {
        return CompactionJobStatusReportArguments.builder()
                .instanceId(instanceId)
                .tableName(tableName)
                .queryType(queryType).reporter(new StandardCompactionJobStatusReporter(out.printStream()));
    }

    private void runCompactionJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType) {
        runCompactionJobStatusReport(argsBuilder(instanceId, tableName, queryType).build());
    }

    private void runCompactionJobStatusReport(CompactionJobStatusReportArguments args) {
        new CompactionJobStatusReport(store.loadCompactionJobStatusStore(args.getInstanceId()), args).run();
        confirmReturnToMainScreen(out, in);
    }

    private MenuOption chooseOptionUntilValid(String message, MenuOption... options) {
        return chooseOne.chooseWithMessageFrom(message, options)
                .chooseUntilChoiceFound(() -> chooseOne.chooseWithMessageFrom(
                        "\nInput not recognised please try again\n", options));
    }
}
