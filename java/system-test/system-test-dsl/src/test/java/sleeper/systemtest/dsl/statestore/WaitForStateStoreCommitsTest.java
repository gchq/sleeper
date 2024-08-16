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
package sleeper.systemtest.dsl.statestore;

import org.junit.jupiter.api.Test;

import sleeper.systemtest.dsl.testutil.drivers.InMemoryStateStoreCommitter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WaitForStateStoreCommitsTest {

    @Test
    void shouldFindOneCommitWasMadeAgainstCorrectTable() {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:14:00Z")));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = withLogs(logs).getRemainingCommitsInPeriod(waitForCommits,
                Instant.parse("2024-08-14T12:00:00Z"),
                Instant.parse("2024-08-14T13:00:00Z"));

        // Then
        assertThat(remainingCommits).isEqualTo(Map.of("test-table", 1));
    }

    @Test
    void shouldFindAllCommitsWereMadeAgainstCorrectTable() {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:14:00Z")),
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:14:30Z")));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = withLogs(logs).getRemainingCommitsInPeriod(waitForCommits,
                Instant.parse("2024-08-14T12:00:00Z"),
                Instant.parse("2024-08-14T13:00:00Z"));

        // Then
        assertThat(remainingCommits).isEmpty();
    }

    @Test
    void shouldFindCommitAgainstWrongTable() {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                commitToTableAtTime("other-table", Instant.parse("2024-08-14T12:14:00Z")));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = withLogs(logs).getRemainingCommitsInPeriod(waitForCommits,
                Instant.parse("2024-08-14T12:00:00Z"),
                Instant.parse("2024-08-14T13:00:00Z"));

        // Then
        assertThat(remainingCommits).isEqualTo(Map.of("test-table", 2));
    }

    @Test
    void shouldFindCommitsAgainstMultipleTables() throws Exception {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                commitToTableAtTime("table-1", Instant.parse("2024-08-14T12:14:00Z")),
                commitToTableAtTime("table-2", Instant.parse("2024-08-14T12:14:30Z")));
        Map<String, Integer> waitForCommits = Map.of("table-1", 2, "table-2", 2);

        // When
        Map<String, Integer> remainingCommits = withLogs(logs).getRemainingCommitsInPeriod(waitForCommits,
                Instant.parse("2024-08-14T12:00:00Z"),
                Instant.parse("2024-08-14T13:00:00Z"));

        // When / Then
        assertThat(remainingCommits).isEqualTo(Map.of("table-1", 1, "table-2", 1));
    }

    @Test
    void shouldIgnoreCommitBeforePeriod() throws Exception {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:14:00Z")),
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:16:00Z")));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = withLogs(logs).getRemainingCommitsInPeriod(waitForCommits,
                Instant.parse("2024-08-14T12:13:00Z"),
                Instant.parse("2024-08-14T12:15:00Z"));

        // When / Then
        assertThat(remainingCommits).isEqualTo(Map.of("test-table", 1));
    }

    @Test
    void shouldIgnoreCommitAfterPeriod() throws Exception {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:14:00Z")),
                commitToTableAtTime("test-table", Instant.parse("2024-08-14T12:16:00Z")));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = withLogs(logs).getRemainingCommitsInPeriod(waitForCommits,
                Instant.parse("2024-08-14T12:15:00Z"),
                Instant.parse("2024-08-14T12:17:00Z"));

        // When / Then
        assertThat(remainingCommits).isEqualTo(Map.of("test-table", 1));
    }

    private StateStoreCommitSummary commitToTableAtTime(String tableId, Instant time) {
        return new StateStoreCommitSummary("test-stream", tableId, "test-commit-type", time);
    }

    private WaitForStateStoreCommitLogs withLogs(List<StateStoreCommitterLogEntry> logs) {
        return new WaitForStateStoreCommitLogs(InMemoryStateStoreCommitter.logsDriver(logs));
    }

}
