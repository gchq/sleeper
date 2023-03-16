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

import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class TablePropertyReport {

    private final ConsoleOutput out;
    private final ConsoleInput in;

    public TablePropertyReport(ConsoleOutput out, ConsoleInput in) {
        this.out = out;
        this.in = in;
    }

    public void print(TableProperties tableProperties) {
        out.println("\n\n Table Property Report \n -------------------------");
        SleeperPropertiesPrettyPrinter.forTableProperties(out.writer())
                .print(tableProperties);
        confirmReturnToMainScreen(out, in);
    }
}
