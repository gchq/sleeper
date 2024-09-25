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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.core.properties.table.TableProperty.COMPACTION_FILES_BATCH_SIZE;
import static sleeper.core.properties.table.TableProperty.TABLE_ID;

public class CompactionJobSerDeTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);

    @BeforeEach
    void setUp() {
        tableProperties.set(COMPACTION_FILES_BATCH_SIZE, "2");
    }

    private CompactionJob.Builder jobForTable() {
        return CompactionJob.builder()
                .tableId(tableProperties.get(TABLE_ID));
    }

    private Schema schemaWithStringKey() {
        return Schema.builder().rowKeyFields(new Field("key", new StringType())).build();
    }

    @Test
    public void shouldSerDeCorrectlyForJobWithNoIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable()
                .jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("partition1").build();
        tableProperties.setSchema(schemaWithStringKey());

        // When
        CompactionJob deserialisedCompactionJob = CompactionJobSerDe.deserialiseFromString(CompactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }

    @Test
    public void shouldSerDeCorrectlyForJobWithIterator() throws IOException {
        // Given
        CompactionJob compactionJob = jobForTable()
                .jobId("compactionJob-1")
                .inputFiles(Arrays.asList("file1", "file2"))
                .outputFile("outputfile")
                .partitionId("partition1")
                .iteratorClassName("Iterator.class")
                .iteratorConfig("config1")
                .build();
        tableProperties.setSchema(schemaWithStringKey());

        // When
        CompactionJob deserialisedCompactionJob = CompactionJobSerDe.deserialiseFromString(CompactionJobSerDe.serialiseToString(compactionJob));

        // Then
        assertThat(deserialisedCompactionJob).isEqualTo(compactionJob);
    }
}
