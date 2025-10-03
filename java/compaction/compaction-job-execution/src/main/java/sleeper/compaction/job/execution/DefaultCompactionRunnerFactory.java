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
package sleeper.compaction.job.execution;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.core.task.CompactionRunnerFactory;
import sleeper.compaction.datafusion.DataFusionCompactionRunner;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.ObjectFactory;
import sleeper.sketches.store.SketchesStore;

import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;

/**
 * Determines which compaction algorithm should be run based on the table and instance configuration properties and
 * other environmental information.
 */
public class DefaultCompactionRunnerFactory implements CompactionRunnerFactory {
    private final ObjectFactory objectFactory;
    private final Configuration configuration;
    private final SketchesStore sketchesStore;

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCompactionRunnerFactory.class);

    public DefaultCompactionRunnerFactory(ObjectFactory objectFactory, Configuration configuration, SketchesStore sketchesStore) {
        this.objectFactory = objectFactory;
        this.configuration = configuration;
        this.sketchesStore = sketchesStore;
    }

    @Override
    public CompactionRunner createCompactor(CompactionJob job, TableProperties tableProperties) {
        if (job.getIteratorClassName() != null) {
            CompactionRunner runner = createJavaRunner();
            LOGGER.info("Table has a Java iterator set, so selecting {} for job ID {}, table {}",
                    runner.getClass().getSimpleName(), job.getId(), tableProperties.getStatus());
            return runner;
        }
        DataEngine engine = tableProperties.getEnumValue(DATA_ENGINE, DataEngine.class);
        CompactionRunner runner = createRunnerForEngine(engine);
        LOGGER.info("Selecting {} for job ID {}, table {}",
                runner.getClass().getSimpleName(), job.getId(), tableProperties.getStatus());
        return runner;
    }

    private CompactionRunner createRunnerForEngine(DataEngine engine) {
        switch (engine) {
            case DATAFUSION:
            case DATAFUSION_COMPACTION_ONLY:
                return new DataFusionCompactionRunner(configuration);
            case JAVA:
            default:
                return createJavaRunner();
        }
    }

    private CompactionRunner createJavaRunner() {
        return new JavaCompactionRunner(objectFactory, configuration, sketchesStore);
    }
}
