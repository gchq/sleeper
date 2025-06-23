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
import static sleeper.compaction.rust.RustCompactionRunner.RUST_MAX_ROW_GROUP_ROWS;
import static sleeper.core.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.PARQUET_WRITER_VERSION;
import static sleeper.core.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;
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
        String[] expectedInputs = new String[]{"/path/to/some/file", "/path/to/other"};
        String[] actual = params.input_files.readBack(String.class, false);
        assertThat(actual).containsExactly(expectedInputs);
        assertThat(params.output_file.get()).isEqualTo(job.getOutputFile());
        assertThat(params.row_key_cols.readBack(String.class, false)).containsExactly("key");
        assertThat(params.row_key_schema.readBack(Integer.class, false)).containsExactly(3);
        assertThat(params.sort_key_cols.len.get()).isEqualTo(0);
        assertThat(params.max_row_group_size.get()).isEqualTo(RUST_MAX_ROW_GROUP_ROWS);
        assertThat(params.max_page_size.get()).isEqualTo(tableProperties.getInt(PAGE_SIZE));
        assertThat(params.compression.get()).isEqualTo(tableProperties.get(COMPRESSION_CODEC));
        assertThat(params.writer_version.get()).isEqualTo(tableProperties.get(PARQUET_WRITER_VERSION));
        assertThat(params.column_truncate_length.get()).isEqualTo(tableProperties.getInt(COLUMN_INDEX_TRUNCATE_LENGTH));
        assertThat(params.stats_truncate_length.get()).isEqualTo(tableProperties.getInt(STATISTICS_TRUNCATE_LENGTH));
        assertThat(params.dict_enc_row_keys.get()).isEqualTo(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS));
        assertThat(params.dict_enc_sort_keys.get()).isEqualTo(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS));
        assertThat(params.dict_enc_values.get()).isEqualTo(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_VALUE_FIELDS));
        assertThat(params.iterator_config.get()).isEmpty();
    }

    private CompactionJobFactory compactionFactory() {
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }
}
