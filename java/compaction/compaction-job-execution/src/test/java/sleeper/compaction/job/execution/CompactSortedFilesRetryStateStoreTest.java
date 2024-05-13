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

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.commit.CompactionJobCommitter;
import sleeper.compaction.job.commit.TimedOutWaitingForFileAssignmentsException;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.PartitionTree;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.exception.FileNotFoundException;
import sleeper.core.statestore.exception.FileReferenceNotAssignedToJobException;
import sleeper.core.util.ExponentialBackoffWithJitter;
import sleeper.core.util.ExponentialBackoffWithJitter.Waiter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.DoubleSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.job.commit.CompactionJobCommitterUtils.updateStateStoreSuccess;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;
import static sleeper.core.statestore.inmemory.StateStoreTestHelper.inMemoryStateStoreWithFixedPartitions;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.noJitter;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.recordWaits;

public class CompactSortedFilesRetryStateStoreTest {

    private static final Instant UPDATE_TIME = Instant.parse("2024-04-26T14:06:00Z");

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = schemaWithKey("key");
    private final PartitionTree partitions = new PartitionsBuilder(schema).singlePartition("root").buildTree();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final StateStore stateStore = inMemoryStateStoreWithFixedPartitions(partitions.getAllPartitions());
    private final FileReferenceFactory fileFactory = FileReferenceFactory.fromUpdatedAt(partitions, UPDATE_TIME);
    private final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    private final List<Duration> foundWaits = new ArrayList<>();
    private Waiter waiter = recordWaits(foundWaits);

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
        updateStateStore(job, 123, noJitter());

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(
                fileFactory.rootFile(job.getOutputFile(), 123));
        assertThat(foundWaits).containsExactly(Duration.ofSeconds(2));
    }

    @Test
    void shouldFailAfterMaxAttemptsWhenFilesNotAssignedToJob() throws Exception {
        // Given
        FileReference file = fileFactory.rootFile("file.parquet", 123);
        stateStore.addFile(file);
        CompactionJob job = createCompactionJobForOneFile(file);

        // When
        assertThatThrownBy(() -> updateStateStore(job, 123, noJitter()))
                .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class)
                .hasCauseInstanceOf(FileReferenceNotAssignedToJobException.class);

        // Then
        assertThat(stateStore.getFileReferences()).containsExactly(file);
        assertThat(foundWaits).containsExactly(
                Duration.ofSeconds(2),
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(32),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1),
                Duration.ofMinutes(1));
    }

    @Test
    void shouldFailWithNoRetriesWhenFileDoesNotExist() throws Exception {
        // Given
        FileReference file = fileFactory.rootFile("file.parquet", 123);
        CompactionJob job = createCompactionJobForOneFile(file);

        // When
        assertThatThrownBy(() -> updateStateStore(job, 123, noJitter()))
                .isInstanceOf(FileNotFoundException.class);

        // Then
        assertThat(stateStore.getFileReferences()).isEmpty();
        assertThat(foundWaits).isEmpty();
    }

    private void updateStateStore(CompactionJob job, long recordsWritten, DoubleSupplier randomJitter) throws Exception {
        updateStateStoreSuccess(job, 123, stateStore,
                CompactionJobCommitter.JOB_ASSIGNMENT_WAIT_ATTEMPTS, backoff(randomJitter));
    }

    private void actionOnWait(WaitAction action) throws Exception {
        Waiter wrapWaiter = waiter;
        waiter = millis -> {
            try {
                action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            wrapWaiter.waitForMillis(millis);
        };
    }

    private CompactionJob createCompactionJobForOneFile(FileReference file) {
        return jobFactory.createCompactionJob(List.of(file), file.getPartitionId());
    }

    private ExponentialBackoffWithJitter backoff(DoubleSupplier randomJitter) {
        return new ExponentialBackoffWithJitter(
                CompactionJobCommitter.JOB_ASSIGNMENT_WAIT_RANGE,
                randomJitter, waiter);
    }

    interface WaitAction {
        void run() throws Exception;
    }
}
