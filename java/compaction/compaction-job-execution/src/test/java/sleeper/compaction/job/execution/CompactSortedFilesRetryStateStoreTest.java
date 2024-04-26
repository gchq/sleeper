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
package sleeper.compaction.job.execution;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.fixJitterSeed;

@ExtendWith(MockitoExtension.class)
public class CompactSortedFilesRetryStateStoreTest {

    private static final Instant UPDATE_TIME = Instant.parse("2024-04-26T14:06:00Z");

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, UPDATE_TIME);
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    @Mock
    Waiter waiter;
    @Captor
    ArgumentCaptor<Long> waitCaptor;

    @BeforeEach
    void setUp() {
        stateStore.fixFileUpdateTime(UPDATE_TIME);
    }

    @Test
    void shouldRetryStateStoreUpdateWhenFilesNotAssignedToJob() throws Exception {
        // Given
        FileReference file = fileFactory.rootFile("file.parquet", 123);
        stateStore.addFile(file);
        CompactionJob job = createCompactionJobForOneFile(file);
        actionOnWait(() -> {
            stateStore.assignJobIds(List.of(AssignJobIdRequest.assignJobOnPartitionToFiles(
                    job.getId(), file.getPartitionId(), List.of(file.getFilename()))));
        });

        // When
        updateStateStoreSuccess(job, 123, waiter);

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(
                fileFactory.rootFile(job.getOutputFile(), 123));
        verify(waiter, times(1)).waitForMillis(730);
    }

    @Test
    void shouldFailAfterMaxAttemptsWhenFilesNotAssignedToJob() throws Exception {
        // Given
        FileReference file = fileFactory.rootFile("file.parquet", 123);
        stateStore.addFile(file);
        CompactionJob job = createCompactionJobForOneFile(file);

        // When
        assertThatThrownBy(() -> updateStateStoreSuccess(job, 123, waiter))
                .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class)
                .hasCauseInstanceOf(FileReferenceNotAssignedToJobException.class);

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(file);
        verify(waiter, times(9)).waitForMillis(waitCaptor.capture());
        assertThat(waitCaptor.getAllValues()).containsExactly(
                730L, 481L, 2549L, 4403L, 9560L, 10662L, 24652L, 118180L, 105501L);
    }

    private void updateStateStoreSuccess(CompactionJob job, long recordsWritten, Waiter waiter) throws Exception {
        CompactSortedFiles.updateStateStoreSuccess(job, 123, stateStore,
                CompactSortedFiles.JOB_ASSIGNMENT_WAIT_ATTEMPTS, backoff(waiter));
    }

    private void actionOnWait(WaitAction action) throws Exception {
        doAnswer(ivocation -> {
            action.run();
            return null;
        }).when(waiter).waitForMillis(anyLong());
    }

    private CompactionJob createCompactionJobForOneFile(FileReference file) {
        return jobFactory.createCompactionJob(List.of(file), file.getPartitionId());
    }

    private ExponentialBackoffWithJitter backoff(Waiter waiter) {
        return new ExponentialBackoffWithJitter(
                CompactSortedFiles.JOB_ASSIGNMENT_WAIT_RANGE,
                fixJitterSeed(), waiter);
    }

    interface WaitAction {
        void run() throws Exception;
    }
}
