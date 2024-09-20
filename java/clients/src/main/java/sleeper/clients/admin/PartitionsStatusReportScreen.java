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
import sleeper.clients.status.report.partitions.PartitionsStatusReporter;
import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.clients.util.console.UserExitedException;
import sleeper.core.statestore.StateStoreException;
import sleeper.splitter.status.PartitionsStatus;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class PartitionsStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final TableSelectHelper tableSelectHelper;
    private final AdminClientPropertiesStore store;

    public PartitionsStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store) {
        this.out = out;
        this.in = in;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
        this.store = store;
    }

    public void chooseTableAndPrint(String instanceId) throws UserExitedException {
        tableSelectHelper.chooseTableOrReturnToMain(instanceId).ifPresent(tableProperties -> {
            try {
                new PartitionsStatusReporter(out.printStream()).report(
                        PartitionsStatus.from(tableProperties, store.loadStateStore(instanceId, tableProperties)));
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
            confirmReturnToMainScreen(out, in);
        });
    }
}
