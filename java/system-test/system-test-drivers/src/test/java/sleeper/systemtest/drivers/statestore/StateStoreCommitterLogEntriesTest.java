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
package sleeper.systemtest.drivers.statestore;

import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.statestore.StateStoreCommitSummary;
import sleeper.clients.status.report.statestore.StateStoreCommitterRunStarted;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterLogEntriesTest {

    @Test
    void shouldCountCommitsByTable() {
        // Given
        StateStoreCommitterLogEntries logs = new StateStoreCommitterLogEntries(List.of(
                commitToTable("table-1"), commitToTable("table-1"),
                commitToTable("table-2")));

        // When / Then
        assertThat(logs.countNumCommitsByTableId(Set.of("table-1", "table-2")))
                .isEqualTo(Map.of("table-1", 2, "table-2", 1));
    }

    @Test
    void shouldCountNoCommits() {
        // Given
        StateStoreCommitterLogEntries logs = new StateStoreCommitterLogEntries(List.of());

        // When / Then
        assertThat(logs.countNumCommitsByTableId(Set.of("test-table")))
                .isEqualTo(Map.of());
    }

    @Test
    void shouldIgnoreCommitForOtherTable() {
        // Given
        StateStoreCommitterLogEntries logs = new StateStoreCommitterLogEntries(List.of(
                commitToTable("table-1"), commitToTable("table-1"),
                commitToTable("table-2")));

        // When / Then
        assertThat(logs.countNumCommitsByTableId(Set.of("table-1")))
                .isEqualTo(Map.of("table-1", 2));
    }

    @Test
    void shouldComputeOverallCommitsPerSecond() {
        // Given
        StateStoreCommitterLogEntries logs = new StateStoreCommitterLogEntries(List.of(
                runStartedAtTime(Instant.parse("2024-08-15T16:17:00Z")),
                commitToTableAtTime("test-table", Instant.parse("2024-08-15T16:17:01Z"))));

        // When / Then
        assertThat(logs.computeOverallCommitsPerSecondByTableId(Set.of("test-table")))
                .isEqualTo(Map.of("test-table", 1.0));
    }

    StateStoreCommitSummary commitToTable(String tableId) {
        return new StateStoreCommitSummary("test-stream", tableId, "test-commit", Instant.now());
    }

    StateStoreCommitSummary commitToTableAtTime(String tableId, Instant time) {
        return new StateStoreCommitSummary("test-stream", tableId, "test-commit", time);
    }

    StateStoreCommitterRunStarted runStartedAtTime(Instant time) {
        return new StateStoreCommitterRunStarted("test-stream", time);
    }
}
