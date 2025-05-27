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
import sleeper.compaction.rust.RustCompactionRunner;
import sleeper.core.properties.model.CompactionMethod;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.ObjectFactory;
import sleeper.sketchesv2.store.SketchesStore;

import static sleeper.core.properties.table.TableProperty.COMPACTION_METHOD;

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
        CompactionMethod method = tableProperties.getEnumValue(COMPACTION_METHOD, CompactionMethod.class);
        CompactionRunner runner = createRunnerForMethod(method);

        // Is an iterator specifed? If so can we support this?
        if (job.getIteratorClassName() != null && !runner.supportsIterators()) {
            LOGGER.debug("Table has an iterator set, which compactor {} doesn't support, falling back to default", runner.getClass().getSimpleName());
            runner = createJavaRunner();
        }

        // Has an experimental DataFusion only iterator been specified? If so, make sure
        // we are using the DataFusion compactor
        if (CompactionJob.DATAFUSION_ITERATOR_NAME.equals(job.getIteratorClassName()) && !(runner instanceof RustCompactionRunner)) {
            throw new IllegalStateException("DataFusion-only iterator specified, but DataFusion compactor not selected for job ID "
                    + job.getId() + " table ID " + job.getTableId());
        }

        LOGGER.info("Selecting {} compactor (language {}) for job ID {} table ID {}", runner.getClass().getSimpleName(), runner.implementationLanguage(), job.getId(), job.getTableId());
        return runner;
    }

    private CompactionRunner createRunnerForMethod(CompactionMethod method) {
        switch (method) {
            case DATAFUSION:
                return new RustCompactionRunner();
            case JAVA:
            default:
                return createJavaRunner();
        }
    }

    private CompactionRunner createJavaRunner() {
        return new JavaCompactionRunner(objectFactory, configuration, sketchesStore);
    }
}
