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
package sleeper.compaction.job;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.core.job.CompactionRunner;
import sleeper.compaction.datafusion.DataFusionCompactionRunner;
import sleeper.compaction.job.execution.DefaultCompactionRunnerFactory;
import sleeper.compaction.job.execution.JavaCompactionRunner;
import sleeper.core.iterator.AgeOffIterator;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.type.LongType;
import sleeper.core.util.ObjectFactory;
import sleeper.sketches.store.NoSketchesStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.table.TableProperty.DATA_ENGINE;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.core.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class DefaultCompactionRunnerFactoryTest {

    InstanceProperties instanceProperties = createTestInstanceProperties();
    TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key", new LongType()));

    @Test
    void shouldSetDataFusionEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION);

        // When / Then
        assertThat(createRunner()).isInstanceOf(DataFusionCompactionRunner.class);
    }

    @Test
    void shouldSetJavaEngine() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.JAVA);

        // When / Then
        assertThat(createRunner()).isInstanceOf(JavaCompactionRunner.class);
    }

    @Test
    void shouldForceJavaEngineWhenCustomTableIteratorIsSet() {
        // Given
        tableProperties.setEnum(DATA_ENGINE, DataEngine.DATAFUSION);
        tableProperties.set(ITERATOR_CLASS_NAME, AgeOffIterator.class.getName());
        tableProperties.set(ITERATOR_CONFIG, "key,1000");

        // When / Then
        assertThat(createRunner()).isInstanceOf(JavaCompactionRunner.class);
    }

    private CompactionRunner createRunner() {
        return new DefaultCompactionRunnerFactory(ObjectFactory.noUserJars(), new Configuration(), new NoSketchesStore())
                .createCompactor(exampleCompactionJob(), tableProperties);
    }

    private CompactionJob exampleCompactionJob() {
        return new CompactionJobFactory(instanceProperties, tableProperties)
                .createCompactionJobWithFilenames("test-job", List.of("test-file.parquet"), "test-partition");
    }

}
