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
package sleeper.compaction.core.task;

import org.junit.jupiter.api.Test;

import sleeper.compaction.core.job.CompactionJob;
import sleeper.compaction.core.job.CompactionJobFactory;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.testutils.FixedTablePropertiesProvider;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.FixedStateStoreProvider;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.util.ThreadSleep;
import sleeper.core.util.ThreadSleepTestHelper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.reducing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.compaction.core.task.StateStoreWaitForFiles.JOB_ASSIGNMENT_WAIT_ATTEMPTS;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.core.properties.testutils.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.core.schema.SchemaTestHelper.createSchemaWithKey;
import static sleeper.core.statestore.testutils.StateStoreUpdatesWrapper.update;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.constantJitterFraction;
import static sleeper.core.util.ExponentialBackoffWithJitterTestHelper.fixJitterSeed;

public class StateStoreWaitForFilesTest {
    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final Schema schema = createSchemaWithKey("key");
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, schema);
    private final InMemoryTransactionLogs transactionLogs = new InMemoryTransactionLogs();
    private final InMemoryTransactionLogStore filesLogStore = transactionLogs.getFilesLogStore();
    private final StateStore stateStore = InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, transactionLogs);
    private final FileReferenceFactory factory = FileReferenceFactory.from(stateStore);
    private final List<Duration> foundWaits = new ArrayList<>();
    private ThreadSleep waiter = ThreadSleepTestHelper.recordWaits(foundWaits);

    @Test
    void shouldSkipWaitIfFilesAreAlreadyAssignedToJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        update(stateStore).addFile(file);
        CompactionJob job = jobForFileAtRoot(file);
        update(stateStore).assignJobIds(List.of(job.createAssignJobIdRequest()));

        // When
        waitForFilesWithAttempts(2, job);

        // Then
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldRetryThenCheckFilesAreAssignedToJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        update(stateStore).addFile(file);
        CompactionJob job = jobForFileAtRoot(file);
        actionAfterWait(() -> {
            update(stateStore).assignJobIds(List.of(job.createAssignJobIdRequest()));
        });

        // When
        waitForFilesWithAttempts(2, job);

        // Then
        assertThat(foundWaits).hasSize(1);
    }

    @Test
    void shouldTimeOutIfFilesAreNeverAssignedToJob() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        update(stateStore).addFile(file);
        CompactionJob job = jobForFileAtRoot(file);

        // When / Then
        assertThatThrownBy(() -> waitForFilesWithAttempts(2, job))
                .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class);
        assertThat(foundWaits).hasSize(1);
    }

    @Test
    void shouldWaitWithExponentialBackoffAndJitter() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        update(stateStore).addFile(file);
        CompactionJob job = jobForFileAtRoot(file);

        // When / Then
        StateStoreWaitForFiles waiter = waiter().withAttempts(JOB_ASSIGNMENT_WAIT_ATTEMPTS, fixJitterSeed());
        assertThatThrownBy(() -> waiter.wait(job, "test-task", "test-job-run"))
                .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class);
        assertThat(foundWaits).containsExactly(
                Duration.parse("PT2.923S"),
                Duration.parse("PT1.924S"),
                Duration.parse("PT10.198S"),
                Duration.parse("PT17.613S"),
                Duration.parse("PT35.852S"),
                Duration.parse("PT19.993S"),
                Duration.parse("PT23.111S"),
                Duration.parse("PT59.09S"),
                Duration.parse("PT52.75S"));
        assertThat(foundWaitsTotal())
                .isEqualTo(Duration.parse("PT3M43.454S"));
    }

    @Test
    void shouldWaitWithAverageExponentialBackoff() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        update(stateStore).addFile(file);
        CompactionJob job = jobForFileAtRoot(file);

        // When / Then
        StateStoreWaitForFiles waiter = waiter().withAttempts(
                JOB_ASSIGNMENT_WAIT_ATTEMPTS, constantJitterFraction(0.5));
        assertThatThrownBy(() -> waiter.wait(job, "test-task", "test-job-run"))
                .isInstanceOf(TimedOutWaitingForFileAssignmentsException.class);
        assertThat(foundWaits).containsExactly(
                Duration.ofSeconds(2),
                Duration.ofSeconds(4),
                Duration.ofSeconds(8),
                Duration.ofSeconds(16),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                Duration.ofSeconds(30));
        assertThat(foundWaitsTotal())
                .isEqualTo(Duration.ofMinutes(3));
    }

    @Test
    void shouldRetryTwiceWhenThrottledQueryingStateStore() throws Exception {
        // Given
        FileReference file = factory.rootFile("test.parquet", 123L);
        update(stateStore).addFile(file);
        CompactionJob job = jobForFileAtRoot(file);
        update(stateStore).assignJobIds(List.of(job.createAssignJobIdRequest()));

        RuntimeException exception = new RuntimeException("Throttled");
        Iterator<RuntimeException> exceptions = List.of(exception, exception).iterator();
        filesLogStore.atStartOfReadTransactions(() -> {
            if (exceptions.hasNext()) {
                throw exceptions.next();
            }
        });

        // When
        waiter().withThrottlingRetriesOnException(exception).wait(job, "test-task", "test-job-run");

        // Then
        assertThat(foundWaits).containsExactly(
                Duration.ofMinutes(1), Duration.ofMinutes(1));
        assertThat(exceptions).isExhausted();
    }

    private Duration foundWaitsTotal() {
        return foundWaits.stream()
                .collect(reducing((Duration a, Duration b) -> a.plus(b)))
                .orElse(Duration.ZERO);
    }

    private CompactionJob jobForFileAtRoot(FileReference... files) {
        return new CompactionJobFactory(instanceProperties, tableProperties).createCompactionJob(List.of(files), "root");
    }

    private void waitForFilesWithAttempts(int attempts, CompactionJob job) throws Exception {
        waiter().withAttempts(attempts).wait(job, "test-task", "test-job-run");
    }

    private StateStoreWaitForFilesTestHelper waiter() {
        return new StateStoreWaitForFilesTestHelper(
                new FixedTablePropertiesProvider(tableProperties),
                FixedStateStoreProvider.singleTable(tableProperties, stateStore),
                CompactionJobTracker.NONE, waiter, Instant::now);
    }

    protected void actionAfterWait(ThreadSleepTestHelper.WaitAction action) throws Exception {
        waiter = ThreadSleepTestHelper.withActionAfterWait(waiter, action);
    }
}
