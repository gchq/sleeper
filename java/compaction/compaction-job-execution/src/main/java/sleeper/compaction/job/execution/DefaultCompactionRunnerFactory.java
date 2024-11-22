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

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionRunnerFactory;
import sleeper.compaction.gpu.GPUCompactionRunner;
import sleeper.compaction.rust.RustCompactionRunner;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.validation.CompactionMethod;
import sleeper.core.util.ObjectFactory;

import static sleeper.core.properties.table.TableProperty.COMPACTION_METHOD;

/**
 * Determines which compaction algorithm should be run based on the table and instance configuration properties and
 * other environmental information.
 */
public class DefaultCompactionRunnerFactory implements CompactionRunnerFactory {
    private final ObjectFactory objectFactory;
    private final Configuration configuration;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCompactionRunnerFactory.class);

    public DefaultCompactionRunnerFactory(ObjectFactory objectFactory, Configuration configuration) {
        this.objectFactory = objectFactory;
        this.configuration = configuration;
    }

    @Override
    public CompactionRunner createCompactor(CompactionJob job, InstanceProperties instanceProperties, TableProperties tableProperties) {
        CompactionMethod method = tableProperties.getEnumValue(COMPACTION_METHOD, CompactionMethod.class);
        CompactionRunner runner = createRunnerForMethod(method);

        // Is an iterator specifed? If so can we support this?
        if (job.getIteratorClassName() != null && !runner.supportsIterators()) {
            LOGGER.warn("Table has an iterator set, which compactor {} doesn't support, falling back to default", runner.getClass().getSimpleName());
            runner = createJavaRunner();
        }

        // Is this compactor compatible with the current instance configuration?
        if (!runner.supportsInstanceConfiguration(instanceProperties)) {
            LOGGER.warn("Selected compactor {} is not compatible with instance configuration, falling back to default", runner.getClass().getSimpleName());
            runner = createJavaRunner();
        }

        LOGGER.info("Selecting {} compactor (language {}) for job ID {} table ID {}", runner.getClass().getSimpleName(), runner.implementationLanguage(), job.getId(), job.getTableId());
        return runner;
    }

    private CompactionRunner createRunnerForMethod(CompactionMethod method) {
        switch (method) {
            case DATAFUSION:
                return new RustCompactionRunner();
            case GPU:
                return new GPUCompactionRunner();
            case JAVA:
            default:
                return createJavaRunner();
        }
    }

    private CompactionRunner createJavaRunner() {
        return new JavaCompactionRunner(objectFactory, configuration);
    }
}
