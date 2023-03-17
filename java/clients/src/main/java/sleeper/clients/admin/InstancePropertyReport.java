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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.format.SleeperPropertiesPrettyPrinter;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;

public class InstancePropertyReport {

    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final AdminConfigStore store;

    public InstancePropertyReport(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.store = store;
    }

    public void print(String instanceId) {
        print(store.loadInstanceProperties(instanceId));
    }

    private void print(InstanceProperties instanceProperties) {
        out.println("\n\n Instance Property Report \n -------------------------");
        SleeperPropertiesPrettyPrinter.forInstanceProperties(out.writer())
                .print(instanceProperties);
        confirmReturnToMainScreen(out, in);
    }
}
