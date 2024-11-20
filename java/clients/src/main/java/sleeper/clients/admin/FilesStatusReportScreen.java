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
import sleeper.clients.status.report.FilesStatusReport;
import sleeper.clients.status.report.filestatus.StandardFileStatusReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.core.properties.table.TableProperties;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class FilesStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final AdminClientPropertiesStore store;
    private final TableSelectHelper tableSelectHelper;

    public FilesStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store) {
        this.out = out;
        this.in = in;
        this.store = store;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseTableAndPrint(String instanceId) {
        tableSelectHelper.chooseTableOrReturnToMain(instanceId).ifPresent(tableProperties -> {
            chooseOptionalArgsAndPrint(instanceId, tableProperties);
            confirmReturnToMainScreen(out, in);
        });
    }

    private void chooseOptionalArgsAndPrint(String instanceId, TableProperties tableProperties) {
        out.println("By default, we report on a maximum of 1000 files that are ready for garbage collection.");
        int maxReadyForGCFiles = 1000;
        String maxGcArg = in.promptLine("If you would like to change this limit, enter a new value now, " +
                "or press enter to use the default: ");
        if (maxGcArg.isEmpty()) {
            out.println("No value entered, defaulting to " + maxReadyForGCFiles);
        } else {
            try {
                maxReadyForGCFiles = Integer.parseInt(maxGcArg);
            } catch (NumberFormatException e) {
                out.println("Failed to convert input to integer, defaulting to " + maxReadyForGCFiles);
            }
        }
        boolean verbose = in.promptLine("Run report in verbose mode? (y/N): ").equalsIgnoreCase("y");

        new FilesStatusReport(store.loadStateStore(instanceId, tableProperties), maxReadyForGCFiles, verbose,
                new StandardFileStatusReporter(out.printStream())).run();
    }
}
