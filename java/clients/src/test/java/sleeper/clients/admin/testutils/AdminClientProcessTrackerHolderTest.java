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
package sleeper.clients.admin.testutils;

import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.InMemoryCompactionJobTracker;
import sleeper.core.tracker.compaction.task.CompactionTaskTracker;
import sleeper.core.tracker.compaction.task.InMemoryCompactionTaskTracker;
import sleeper.ingest.core.job.status.IngestJobStatusStore;
import sleeper.ingest.core.task.IngestTaskTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AdminClientProcessTrackerHolderTest extends AdminClientMockStoreBase {

    @Test
    void shouldSetCompactionJobTracker() {
        // Given
        CompactionJobTracker store = new InMemoryCompactionJobTracker();
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().tracker(store);

        // Then
        assertThat(runner.trackers().loadCompactionJobTracker(properties))
                .isSameAs(store);
    }

    @Test
    void shouldSetCompactionTaskTracker() {
        // Given
        InMemoryCompactionTaskTracker tracker = new InMemoryCompactionTaskTracker();
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().tracker(tracker);

        // Then
        assertThat(runner.trackers().loadCompactionTaskTracker(properties))
                .isSameAs(tracker);
    }

    @Test
    void shouldSetIngestJobStatusStore() {
        // Given
        IngestJobStatusStore store = mock(IngestJobStatusStore.class);
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().tracker(store);

        // Then
        assertThat(runner.trackers().loadIngestJobStatusStore(properties))
                .isSameAs(store);
    }

    @Test
    void shouldSetIngestTaskStatusStore() {
        // Given
        IngestTaskTracker store = mock(IngestTaskTracker.class);
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().tracker(store);

        // Then
        assertThat(runner.trackers().loadIngestTaskTracker(properties))
                .isSameAs(store);
    }

    @Test
    void shouldReturnNoCompactionJobTracker() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().trackers().loadCompactionJobTracker(properties))
                .isSameAs(CompactionJobTracker.NONE);
    }

    @Test
    void shouldReturnNoCompactionTaskTracker() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().trackers().loadCompactionTaskTracker(properties))
                .isSameAs(CompactionTaskTracker.NONE);
    }

    @Test
    void shouldReturnNoIngestJobStatusStore() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().trackers().loadIngestJobStatusStore(properties))
                .isSameAs(IngestJobStatusStore.NONE);
    }

    @Test
    void shouldReturnNoIngestTaskStatusStore() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().trackers().loadIngestTaskTracker(properties))
                .isSameAs(IngestTaskTracker.NONE);
    }
}
