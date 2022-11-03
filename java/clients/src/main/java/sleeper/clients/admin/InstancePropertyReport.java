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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.console.ConsoleOutput;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;

public class InstancePropertyReport {

    private final ConsoleOutput out;
    private final AdminConfigStore store;

    public InstancePropertyReport(ConsoleOutput out, AdminConfigStore store) {
        this.out = out;
        this.store = store;
    }

    public void print(String instanceId) {
        print(store.loadInstanceProperties(instanceId));
    }

    private void print(InstanceProperties instanceProperties) {

        NavigableMap<Object, Object> orderedProperties = SleeperPropertyUtils.orderedPropertyMapWithIncludes(
                instanceProperties, Arrays.asList(UserDefinedInstanceProperty.values()));
        out.println("\n\n Instance Property Report \n -------------------------");
        for (Map.Entry<Object, Object> entry : orderedProperties.entrySet()) {
            out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
