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
package sleeper.compaction.rust;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.compaction.rust.RustBridge.FFICompactionParams;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;

public class RustCompactionRunnerTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
    private final RustBridge.Compaction nativeLib;
    private final jnr.ffi.Runtime runtime;

    public RustCompactionRunnerTest() throws IOException {
        nativeLib = RustBridge.getRustCompactor();
        runtime = jnr.ffi.Runtime.getRuntime(nativeLib);
    }

    @Test
    public void shouldCreateFFICompactionParamsWithNoIterator() {
        // Given
        Schema schema = createSchemaWithKey("key", new StringType());
        tableProperties.setSchema(schema);
        tableProperties.set(TableProperty.ITERATOR_CLASS_NAME, null);
        tableProperties.set(TableProperty.ITERATOR_CONFIG, null);
        CompactionJob job = compactionFactory().createCompactionJobWithFilenames(UUID.randomUUID().toString(), List.of("/path/to/some/file", "/path/to/other"), UUID.randomUUID().toString());
        Region compactionRegion = new Region(new Range(new Field("key", new StringType()), "a", "k"));

        // When
        FFICompactionParams params = RustCompactionRunner.createFFIParams(job, tableProperties, compactionRegion, null, runtime);

        // Then
        assertThat(params.override_aws_config.get()).isFalse();

    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }
}
