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
package sleeper.compaction.job;

import org.junit.jupiter.api.Test;

import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.instance.CommonProperty.FILE_SYSTEM;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

public class CompactionJobFactoryTest {
    private static final Schema DEFAULT_SCHEMA = schemaWithKey("key");
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, DEFAULT_SCHEMA);
    private final PartitionTree partitionTree = new PartitionsBuilder(DEFAULT_SCHEMA)
            .singlePartition("root").buildTree();
    private final FileReferenceFactory fileFactory = FileReferenceFactory.from(partitionTree);

    @Test
    void shouldCreateCompactionJobFromFileReferences() {
        // Given
        instanceProperties.set(FILE_SYSTEM, "file://");
        instanceProperties.set(DATA_BUCKET, "test-data-bucket");
        tableProperties.set(TABLE_ID, "test-table-id");
        FileReference file1 = fileFactory.rootFile("file1.parquet", 123L);
        FileReference file2 = fileFactory.rootFile("file2.parquet", 456L);
        CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties, () -> "job1");

        // When
        CompactionJob job = jobFactory.createCompactionJob(List.of(file1, file2), "root");

        // Then
        assertThat(job).isEqualTo(CompactionJob.builder()
                .jobId("job1")
                .inputFiles(List.of("file1.parquet", "file2.parquet"))
                .partitionId("root")
                .outputFile("file://test-data-bucket/test-table-id/data/partition_root/job1.parquet")
                .tableId("test-table-id")
                .build());
    }
}
