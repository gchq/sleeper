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
package sleeper.clients.report;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

/**
 * Reports which tables are in an instance of Sleeper.
 */
public class TableNamesReport {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final TableIndex tableIndex;

    public TableNamesReport(ConsoleOutput out, ConsoleInput in, TableIndex tableIndex) {
        this.out = out;
        this.in = in;
        this.tableIndex = tableIndex;
    }

    /**
     * Prints the report.
     */
    public void print() {
        print(tableIndex.streamAllTables().collect(Collectors.toList()), true);
    }

    /**
     * Prints the report.
     *
     * @param confirmReturn true if this is occurring in the admin client, and we want to wait for the user to confirm
     *                      before returning to the main screen
     */
    public void print(boolean confirmReturn) {
        print(tableIndex.streamAllTables().collect(Collectors.toList()), confirmReturn);
    }

    private void print(List<TableStatus> allTables, boolean confirmReturn) {
        out.println("\n\nTable Names\n----------------------------------");
        allTables.stream()
                .filter(TableStatus::isOnline)
                .map(TableStatus::getTableName)
                .forEach(out::println);

        allTables.stream()
                .filter(not(TableStatus::isOnline))
                .map(TableStatus::getTableName)
                .forEach(tableName -> out.println(tableName + " (offline)"));
        if (confirmReturn) {
            confirmReturnToMainScreen(out, in);
        }
    }
}
