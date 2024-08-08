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

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionRunner;
import sleeper.compaction.task.CompactionAlgorithmSelector;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.validation.CompactionMethod;
import sleeper.statestore.StateStoreProvider;

import static sleeper.configuration.properties.table.TableProperty.COMPACTION_METHOD;

/**
 * Determines which compaction algorithm should be run based on the table and instance configuration properties and
 * other environmental information.
 */
public class DefaultSelector implements CompactionAlgorithmSelector {
    private final TablePropertiesProvider tablePropertiesProvider;
    private final ObjectFactory objectFactory;
    private final StateStoreProvider stateStoreProvider;
    private final Configuration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSelector.class);

    public DefaultSelector(
            TablePropertiesProvider tablePropertiesProvider,
            StateStoreProvider stateStoreProvider, ObjectFactory objectFactory, Configuration configuration) {
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.objectFactory = objectFactory;
        this.stateStoreProvider = stateStoreProvider;
        this.configuration = configuration;
    }

    @Override
    public CompactionRunner chooseCompactor(CompactionJob job) {
        TableProperties tableProperties = tablePropertiesProvider
                .getById(job.getTableId());
        CompactionMethod method = tableProperties.getEnumValue(COMPACTION_METHOD, CompactionMethod.class);
        CompactionRunner runner = createRunnerForMethod(method);

        // Is an iterator specifed? If so can we support this?
        if (job.getIteratorClassName() != null && !runner.supportsIterators()) {
            LOGGER.debug("Table has an iterator set, which compactor {} doesn't support, falling back to default", runner.getClass().getSimpleName());
            runner = createJavaRunner();
        }

        LOGGER.info("Selecting {} compactor (language {}) for job ID {} table ID {}", runner.getClass().getSimpleName(), runner.implementationLanguage(), job.getId(), job.getTableId());
        return runner;
    }

    private CompactionRunner createRunnerForMethod(CompactionMethod method) {
        switch (method) {
            case GPU:
                return new GPUCompaction(tablePropertiesProvider, stateStoreProvider);
            case RUST:
                return new RustCompaction(tablePropertiesProvider, stateStoreProvider);
            case JAVA:
            default:
                return createJavaRunner();
        }
    }

    private CompactionRunner createJavaRunner() {
        return new StandardCompactor(tablePropertiesProvider, stateStoreProvider, objectFactory, configuration);
    }
}
