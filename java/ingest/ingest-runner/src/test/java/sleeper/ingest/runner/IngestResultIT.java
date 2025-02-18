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

package sleeper.ingest.runner;

import org.junit.jupiter.api.Test;

import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogStateStore;
import sleeper.core.statestore.testutils.InMemoryTransactionLogs;
import sleeper.ingest.core.IngestResult;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.runner.testutils.IngestRecordsTestDataHelper.getRecords;

class IngestResultIT extends IngestRecordsTestBase {
    @Test
    void shouldReturnNumberOfRecordsFromIngestResult() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore();

        // When
        IngestResult result = ingestRecords(stateStore, getRecords());

        // Then
        assertThat(result.getRecordsWritten())
                .isEqualTo(2L);
    }

    @Test
    void shouldReturnFileReferenceListFromIngestResult() throws Exception {
        // Given
        StateStore stateStore = initialiseStateStore();

        // When
        IngestResult result = ingestRecords(stateStore, getRecords());

        // Then
        assertThat(result.getFileReferenceList())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactlyInAnyOrderElementsOf(stateStore.getFileReferences());
    }

    private StateStore initialiseStateStore() {
        return InMemoryTransactionLogStateStore.createAndInitialise(tableProperties, new InMemoryTransactionLogs());
    }
}
