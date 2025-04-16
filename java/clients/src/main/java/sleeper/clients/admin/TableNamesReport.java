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
package sleeper.clients.admin;

import sleeper.clients.util.console.ConsoleInput;
import sleeper.core.table.TableIndex;
import sleeper.core.table.TableStatus;
import sleeper.core.util.ConsoleOutput;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class TableNamesReport {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final TableIndex tableIndex;

    public TableNamesReport(ConsoleOutput out, ConsoleInput in, TableIndex tableIndex) {
        this.out = out;
        this.in = in;
        this.tableIndex = tableIndex;
    }

    public void print() {
        print(tableIndex.streamAllTables().collect(Collectors.toList()));
    }

    private void print(List<TableStatus> allTables) {
        out.println("\n\nTable Names\n----------------------------------");
        allTables.stream()
                .filter(TableStatus::isOnline)
                .map(TableStatus::getTableName)
                .forEach(out::println);

        allTables.stream()
                .filter(not(TableStatus::isOnline))
                .map(TableStatus::getTableName)
                .forEach(tableName -> out.println(tableName + " (offline)"));
        confirmReturnToMainScreen(out, in);
    }
}
