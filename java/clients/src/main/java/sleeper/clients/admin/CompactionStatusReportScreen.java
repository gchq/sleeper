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

import sleeper.console.ConsoleHelper;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.MenuOption;
import sleeper.status.report.CompactionJobStatusReport;
import sleeper.status.report.CompactionTaskStatusReport;
import sleeper.status.report.compaction.job.CompactionJobStatusReportArguments;
import sleeper.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.status.report.compaction.task.CompactionTaskQuery;
import sleeper.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.status.report.job.query.JobQuery;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class CompactionStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminConfigStore store;
    private final TableSelectHelper tableSelectHelper;

    public CompactionStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseArgsAndPrint(String instanceId) {
        out.clearScreen("");
        consoleHelper.chooseOptionUntilValid("Which compaction report would you like to run?",
                new MenuOption("Compaction Job Status Report", () ->
                        chooseArgsForCompactionJobStatusReport(instanceId)),
                new MenuOption("Compaction Task Status Report", () ->
                        chooseArgsForCompactionTaskStatusReport(instanceId))
        ).run();
    }

    private void chooseArgsForCompactionTaskStatusReport(String instanceId) {
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use?",
                new MenuOption("All", () ->
                        runCompactionTaskStatusReport(instanceId, CompactionTaskQuery.ALL)),
                new MenuOption("Unfinished", () ->
                        runCompactionTaskStatusReport(instanceId, CompactionTaskQuery.UNFINISHED))
        ).run();
    }

    private void chooseArgsForCompactionJobStatusReport(String instanceId) {
        String tableName = tableSelectHelper.chooseTable().getEntered();
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use?",
                new MenuOption("All", () ->
                        runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.ALL)),
                new MenuOption("Unfinished", () ->
                        runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.UNFINISHED)),
                new MenuOption("Detailed (specific job)", () ->
                        chooseJobIdAndRunDetailedStatusReport(instanceId, tableName)),
                new MenuOption("Range", () ->
                        chooseRangeAndRunRangeStatusReport(instanceId, tableName))
        ).run();
    }

    private void chooseJobIdAndRunDetailedStatusReport(String instanceId, String tableName) {
        String jobId = in.promptLine("Enter the jobId of the job you want to view the details for: ");
        runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.DETAILED, jobId);
    }

    private void chooseRangeAndRunRangeStatusReport(String instanceId, String tableName) {
        String startRange = in.promptLine("Enter the start range in the format yyyyMMddhhmmss: ");
        String endRange = in.promptLine("Enter the end range in the format yyyyMMddhhmmss: ");
        runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.RANGE, startRange + "," + endRange);
    }

    private CompactionJobStatusReportArguments.Builder argsBuilder(String instanceId, String tableName, JobQuery.Type queryType) {
        return CompactionJobStatusReportArguments.builder()
                .instanceId(instanceId).tableName(tableName)
                .queryType(queryType).reporter(new StandardCompactionJobStatusReporter(out.printStream()));
    }

    private void runCompactionJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType, String queryParams) {
        runCompactionJobStatusReport(argsBuilder(instanceId, tableName, queryType).queryParameters(queryParams).build());
    }

    private void runCompactionJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType) {
        runCompactionJobStatusReport(argsBuilder(instanceId, tableName, queryType).build());
    }

    private void runCompactionJobStatusReport(CompactionJobStatusReportArguments args) {
        new CompactionJobStatusReport(store.loadCompactionJobStatusStore(args.getInstanceId()), args).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runCompactionTaskStatusReport(String instanceId, CompactionTaskQuery queryType) {
        new CompactionTaskStatusReport(store.loadCompactionTaskStatusStore(instanceId),
                new StandardCompactionTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}
