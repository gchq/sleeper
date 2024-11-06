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

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.compaction.core.task.CompactionTaskStatusStore;
import sleeper.compaction.core.testutils.InMemoryCompactionJobStatusStore;
import sleeper.compaction.core.testutils.InMemoryCompactionTaskStatusStore;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.task.IngestTaskStatusStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AdminClientStatusStoreHolderTest extends AdminClientMockStoreBase {

    @Test
    void shouldSetCompactionJobStatusStore() {
        // Given
        CompactionJobStatusStore store = new InMemoryCompactionJobStatusStore();
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().statusStore(store);

        // Then
        assertThat(runner.statusStores().loadCompactionJobStatusStore(properties))
                .isSameAs(store);
    }

    @Test
    void shouldSetCompactionTaskStatusStore() {
        // Given
        InMemoryCompactionTaskStatusStore store = new InMemoryCompactionTaskStatusStore();
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().statusStore(store);

        // Then
        assertThat(runner.statusStores().loadCompactionTaskStatusStore(properties))
                .isSameAs(store);
    }

    @Test
    void shouldSetIngestJobStatusStore() {
        // Given
        IngestJobStatusStore store = mock(IngestJobStatusStore.class);
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().statusStore(store);

        // Then
        assertThat(runner.statusStores().loadIngestJobStatusStore(properties))
                .isSameAs(store);
    }

    @Test
    void shouldSetIngestTaskStatusStore() {
        // Given
        IngestTaskStatusStore store = mock(IngestTaskStatusStore.class);
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When
        RunAdminClient runner = runClient().statusStore(store);

        // Then
        assertThat(runner.statusStores().loadIngestTaskStatusStore(properties))
                .isSameAs(store);
    }

    @Test
    void shouldReturnNoCompactionJobStatusStore() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().statusStores().loadCompactionJobStatusStore(properties))
                .isSameAs(CompactionJobStatusStore.NONE);
    }

    @Test
    void shouldReturnNoCompactionTaskStatusStore() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().statusStores().loadCompactionTaskStatusStore(properties))
                .isSameAs(CompactionTaskStatusStore.NONE);
    }

    @Test
    void shouldReturnNoIngestJobStatusStore() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().statusStores().loadIngestJobStatusStore(properties))
                .isSameAs(IngestJobStatusStore.NONE);
    }

    @Test
    void shouldReturnNoIngestTaskStatusStore() {
        // Given
        InstanceProperties properties = createValidInstanceProperties();
        setInstanceProperties(properties);

        // When / Then
        assertThat(runClient().statusStores().loadIngestTaskStatusStore(properties))
                .isSameAs(IngestTaskStatusStore.NONE);
    }
}
