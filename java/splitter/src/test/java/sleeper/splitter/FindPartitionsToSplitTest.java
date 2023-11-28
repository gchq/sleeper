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

package sleeper.splitter;

import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.IntType;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.core.statestore.StateStore;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class FindPartitionsToSplitTest {
    private static final Schema SCHEMA = Schema.builder().rowKeyFields(new Field("key", new IntType())).build();
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(SCHEMA);
    private final FileInfoFactory fileInfoFactory = FileInfoFactory.from(SCHEMA, stateStore);
    private final String tableId = tableProperties.get(TABLE_ID);

    @Test
    public void shouldSendJobIfAPartitionSizeGoesBeyondThreshold() throws Exception {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
        stateStore.addFile(fileInfoFactory.rootFile("file-1.parquet", 300L));
        stateStore.addFile(fileInfoFactory.rootFile("file-2.parquet", 200L));

        // When
        List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

        // Then
        assertThat(jobs).containsExactly(
                new SplitPartitionJobDefinition(tableId,
                        stateStore.getAllPartitions().get(0),
                        List.of("file-1.parquet", "file-2.parquet")));
    }

    @Test
    public void shouldNotPutMessagesOnAQueueIfPartitionsAreAllUnderThreshold() throws Exception {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 501);
        stateStore.addFile(fileInfoFactory.rootFile("file-1.parquet", 300L));
        stateStore.addFile(fileInfoFactory.rootFile("file-2.parquet", 200L));

        // When
        List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

        // Then
        assertThat(jobs).isEmpty();
    }

    @Test
    public void shouldLimitNumberOfFilesInJobAccordingToTheMaximum() throws Exception {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 2);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
        stateStore.addFile(fileInfoFactory.rootFile("file-1.parquet", 200L));
        stateStore.addFile(fileInfoFactory.rootFile("file-2.parquet", 200L));
        stateStore.addFile(fileInfoFactory.rootFile("file-3.parquet", 200L));

        // When
        List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

        // Then
        assertThat(jobs).containsExactly(
                new SplitPartitionJobDefinition(tableId,
                        stateStore.getAllPartitions().get(0),
                        List.of("file-1.parquet", "file-2.parquet")));
    }

    @Test
    public void shouldPrioritiseFilesContainingTheLargestNumberOfRecords() throws Exception {
        // Given
        instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 2);
        tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
        stateStore.addFile(fileInfoFactory.rootFile("file-1.parquet", 100L));
        stateStore.addFile(fileInfoFactory.rootFile("file-2.parquet", 200L));
        stateStore.addFile(fileInfoFactory.rootFile("file-3.parquet", 300L));

        // When
        List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

        // Then
        assertThat(jobs).containsExactly(
                new SplitPartitionJobDefinition(tableId,
                        stateStore.getAllPartitions().get(0),
                        List.of("file-3.parquet", "file-2.parquet")));
    }

    private List<SplitPartitionJobDefinition> findPartitionsToSplit() throws Exception {
        List<SplitPartitionJobDefinition> jobs = new ArrayList<>();
        new FindPartitionsToSplit(instanceProperties, tableProperties, stateStore, jobs::add).run();
        return jobs;
    }
}
