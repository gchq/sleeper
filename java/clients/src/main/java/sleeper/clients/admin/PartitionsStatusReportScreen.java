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

import sleeper.configuration.properties.table.TableProperties;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.UserExitedException;
import sleeper.console.menu.Chosen;
import sleeper.console.menu.ConsoleChoice;
import sleeper.statestore.StateStoreException;
import sleeper.status.report.partitions.PartitionsStatus;
import sleeper.status.report.partitions.PartitionsStatusReporter;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class PartitionsStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final TableSelectHelper tableSelectHelper;
    private final AdminConfigStore store;

    public PartitionsStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.tableSelectHelper = new TableSelectHelper(out, in);
        this.store = store;
    }

    public void chooseTableAndPrint(String instanceId) throws UserExitedException {
        Chosen<ConsoleChoice> chosen = tableSelectHelper.chooseTable();
        if (chosen.getChoice().isEmpty()) {
            String tableName = chosen.getEntered();
            TableProperties tableProperties = store.loadTableProperties(instanceId, tableName);
            if (tableProperties == null) {
                out.println();
                out.printf("Error: Properties for table \"%s\" could not be found", tableName);
            } else {
                try {
                    new PartitionsStatusReporter(out.printStream()).report(
                            PartitionsStatus.from(tableProperties, store.loadStateStore(instanceId, tableName)));

                } catch (StateStoreException e) {
                    throw new RuntimeException(e);
                }
            }
            confirmReturnToMainScreen(out, in);
        }
    }
}
