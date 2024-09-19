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

package sleeper.splitter.find;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.statestore.FixedStateStoreProvider;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.SplitFileReference;
import sleeper.core.statestore.StateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.PartitionSplittingProperty.MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.PARTITION_SPLIT_THRESHOLD;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithSinglePartition;

public class FindPartitionsToSplitTest {
    private static final Schema SCHEMA = schemaWithKey("key", new LongType());
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, SCHEMA);
    private final StateStore stateStore = inMemoryStateStoreWithSinglePartition(SCHEMA);
    private FileReferenceFactory fileReferenceFactory = fileReferenceFactory();
    private final String tableId = tableProperties.get(TABLE_ID);

    @Nested
    @DisplayName("Create partition splitting jobs")
    class CreateJobs {

        @Test
        public void shouldSendJobIfAPartitionSizeGoesBeyondThreshold() throws Exception {
            // Given
            instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
            stateStore.addFile(fileReferenceFactory.rootFile("file-1.parquet", 300L));
            stateStore.addFile(fileReferenceFactory.rootFile("file-2.parquet", 200L));

            // When
            List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

            // Then
            assertThat(jobs).containsExactly(
                    new SplitPartitionJobDefinition(tableId,
                            partitionTree().getRootPartition(),
                            List.of("file-1.parquet", "file-2.parquet")));
        }

        @Test
        public void shouldNotSendJobIfPartitionsAreAllUnderThreshold() throws Exception {
            // Given
            instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 501);
            stateStore.addFile(fileReferenceFactory.rootFile("file-1.parquet", 300L));
            stateStore.addFile(fileReferenceFactory.rootFile("file-2.parquet", 200L));

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
            stateStore.addFile(fileReferenceFactory.rootFile("file-1.parquet", 200L));
            stateStore.addFile(fileReferenceFactory.rootFile("file-2.parquet", 200L));
            stateStore.addFile(fileReferenceFactory.rootFile("file-3.parquet", 200L));

            // When
            List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

            // Then
            assertThat(jobs).containsExactly(
                    new SplitPartitionJobDefinition(tableId,
                            partitionTree().getRootPartition(),
                            List.of("file-1.parquet", "file-2.parquet")));
        }

        @Test
        public void shouldPrioritiseFilesContainingTheLargestNumberOfRecords() throws Exception {
            // Given
            instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 2);
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);
            stateStore.addFile(fileReferenceFactory.rootFile("file-1.parquet", 100L));
            stateStore.addFile(fileReferenceFactory.rootFile("file-2.parquet", 200L));
            stateStore.addFile(fileReferenceFactory.rootFile("file-3.parquet", 300L));

            // When
            List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

            // Then
            assertThat(jobs).containsExactly(
                    new SplitPartitionJobDefinition(tableId,
                            partitionTree().getRootPartition(),
                            List.of("file-3.parquet", "file-2.parquet")));
        }
    }

    @Nested
    @DisplayName("Handle files split over multiple metadata records")
    class HandleSplitFiles {

        @BeforeEach
        void setUp() throws Exception {
            // Given we have two leaf partitions
            setPartitions(builder -> builder.rootFirst("root")
                    .splitToNewChildren("root", "L", "R", 50L));
            // And we have a file split over the two leaves, so that each leaf has approximately 300 records
            FileReference file = fileReferenceFactory.rootFile("split.parquet", 600L);
            stateStore.addFiles(List.of(
                    SplitFileReference.referenceForChildPartition(file, "L"),
                    SplitFileReference.referenceForChildPartition(file, "R")));
        }

        @Test
        void shouldNotIncludeSplitFileWhenCreatingPartitionSplittingJob() throws Exception {
            // Given the left partition is over the splitting threshold without the split file
            stateStore.addFile(fileReferenceFactory.partitionFile("L", "left.parquet", 600L));
            instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);

            // When we plan the splitting
            List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

            // Then only the whole file is included in the job,
            // since the split file does not contain an accurate sketch for its partition
            assertThat(jobs).containsExactly(
                    new SplitPartitionJobDefinition(tableId,
                            partitionTree().getPartition("L"),
                            List.of("left.parquet")));
        }

        @Test
        void shouldNotSplitPartitionWhenAFileWithRecordsInAnotherPartitionWouldPutItOverTheLimit() throws Exception {
            // Given the left partition would be over the splitting threshold if we included the split file
            stateStore.addFile(fileReferenceFactory.partitionFile("L", "left.parquet", 300L));
            instanceProperties.setNumber(MAX_NUMBER_FILES_IN_PARTITION_SPLITTING_JOB, 10);
            tableProperties.setNumber(PARTITION_SPLIT_THRESHOLD, 500);

            // When we plan the splitting
            List<SplitPartitionJobDefinition> jobs = findPartitionsToSplit();

            // Then there are no jobs created,
            // since we would not be able to include the split file in partition splitting without an accurate sketch
            assertThat(jobs).isEmpty();
        }
    }

    private List<SplitPartitionJobDefinition> findPartitionsToSplit() throws Exception {
        List<SplitPartitionJobDefinition> jobs = new ArrayList<>();
        new FindPartitionsToSplit(instanceProperties, new FixedStateStoreProvider(tableProperties, stateStore), jobs::add)
                .run(tableProperties);
        return jobs;
    }

    private void setPartitions(Consumer<PartitionsBuilder> config) throws Exception {
        PartitionsBuilder builder = new PartitionsBuilder(tableProperties.getSchema());
        config.accept(builder);
        stateStore.initialise(builder.buildList());
        fileReferenceFactory = fileReferenceFactory();
    }

    private PartitionTree partitionTree() throws Exception {
        return new PartitionTree(stateStore.getAllPartitions());
    }

    private FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(stateStore);
    }
}
