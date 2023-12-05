/*
 * Copyright 2022-2023 Crown Copyright
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

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionOutputFileNameFactoryTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schemaWithKey("key"));

    @Test
    void shouldGenerateCompactionOutputFilename() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, "data-bucket");
        tableProperties.set(TABLE_ID, "test-table");
        assertThat(factory().jobPartitionFile("test-job", "test-partition"))
                .isEqualTo("file://data-bucket/test-table/partition_test-partition/test-job.parquet");
    }

    @Test
    void shouldGenerateIndexedCompactionOutputFilename() {
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, "data-bucket");
        tableProperties.set(TABLE_ID, "test-table");
        assertThat(factory().jobPartitionFile("test-job", "test-partition", 12))
                .isEqualTo("file://data-bucket/test-table/partition_test-partition/test-job-12.parquet");
    }

    private CompactionOutputFileNameFactory factory() {
        return CompactionOutputFileNameFactory.forTable(instanceProperties, tableProperties);
    }
}
