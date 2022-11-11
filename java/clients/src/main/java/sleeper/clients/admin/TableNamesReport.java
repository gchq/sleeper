/*
 * Copyright 2022 Crown Copyright
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

import java.util.List;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class TableNamesReport {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final AdminConfigStore store;

    public TableNamesReport(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.store = store;
    }

    public void print(String instanceId) {
        print(store.listTables(instanceId));
    }

    private void print(List<String> tableNames) {

        out.println("\n\n Table Names Report \n -------------------------");
        for (String tableName : tableNames) {
            out.println(tableName);
        }
        confirmReturnToMainScreen(out, in);
    }
}
