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
package sleeper.compaction.job.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.execution.CompactionTask.CompactionRunner;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.statestore.StateStoreProvider;

/**
 * Determines which compaction algorithm should be run based on the table and instance configuration properties and
 * other environmental information.
 */
public class CompactionAlgorithmSelector {
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionAlgorithmSelector.class);

    public CompactionAlgorithmSelector(
            InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, ObjectFactory objectFactory) {
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.objectFactory = objectFactory;
        this.stateStoreProvider = stateStoreProvider;
    }

    public CompactionRunner chooseCompactor(CompactionJob job) {
        return new StandardCompactor(instanceProperties, tablePropertiesProvider, stateStoreProvider, objectFactory);
    }
}
