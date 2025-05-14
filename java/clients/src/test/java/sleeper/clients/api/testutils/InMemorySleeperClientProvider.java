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
package sleeper.clients.api.testutils;

import sleeper.clients.api.SleeperClient;
import sleeper.clients.api.SleeperClientProvider;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class InMemorySleeperClientProvider implements SleeperClientProvider {

    private final Map<String, InMemorySleeperInstance> instanceById = new HashMap<>();

    public InMemorySleeperClientProvider(InMemorySleeperInstance... instances) {
        for (InMemorySleeperInstance instance : instances) {
            instanceById.put(instance.properties().get(ID), instance);
        }
    }

    @Override
    public SleeperClient createClientForInstance(String instanceId) {
        return instanceById.get(instanceId).sleeperClientBuilder().build();
    }

    @Override
    public SleeperClient createClientForInstance(InstanceProperties instanceProperties) {
        return createClientForInstance(instanceProperties.get(ID));
    }

}
