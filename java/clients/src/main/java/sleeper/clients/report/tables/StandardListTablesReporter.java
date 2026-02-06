/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.report.tables;

import sleeper.core.table.TableStatus;

import java.io.PrintStream;
import java.util.stream.Stream;

/**
 * Returns tables to the user on the console.
 */
public class StandardListTablesReporter implements ListTablesReporter {

    private final PrintStream out;

    public StandardListTablesReporter() {
        this(System.out);
    }

    public StandardListTablesReporter(PrintStream out) {
        this.out = out;
    }

    @Override
    public void report(Stream<TableStatus> tables) {
        tables.forEach(table -> {
            out.format("name = \"%s\", id = \"%s\"%n", table.getTableName(), table.getTableUniqueId());
        });
    }
}
