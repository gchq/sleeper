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

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WaitForStateStoreCommitLogsTest {

    @Test
    void shouldFindOneCommitWasMadeAgainstCorrectTable() {
        // Given
        StateStoreCommitterLogs logs = logsWithTableCommits(Map.of("test-table", 1));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = WaitForStateStoreCommitLogs.getRemainingCommits(waitForCommits, logs);

        // Then
        assertThat(remainingCommits).isEqualTo(Map.of("test-table", 1));
    }

    @Test
    void shouldFindAllCommitsWereMadeAgainstCorrectTable() {
        // Given
        StateStoreCommitterLogs logs = logsWithTableCommits(Map.of("test-table", 2));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = WaitForStateStoreCommitLogs.getRemainingCommits(waitForCommits, logs);

        // Then
        assertThat(remainingCommits).isEmpty();
    }

    @Test
    void shouldFindCommitAgainstWrongTable() {
        // Given
        StateStoreCommitterLogs logs = logsWithTableCommits(Map.of("other-table", 1));
        Map<String, Integer> waitForCommits = Map.of("test-table", 2);

        // When
        Map<String, Integer> remainingCommits = WaitForStateStoreCommitLogs.getRemainingCommits(waitForCommits, logs);

        // Then
        assertThat(remainingCommits).isEqualTo(Map.of("test-table", 2));
    }

    @Test
    void shouldFindCommitsAgainstMultipleTables() throws Exception {
        // Given
        StateStoreCommitterLogs logs = logsWithTableCommits(Map.of("table-1", 1, "table-2", 1));
        Map<String, Integer> waitForCommits = Map.of("table-1", 2, "table-2", 2);

        // When
        Map<String, Integer> remainingCommits = WaitForStateStoreCommitLogs.getRemainingCommits(waitForCommits, logs);

        // When / Then
        assertThat(remainingCommits).isEqualTo(Map.of("table-1", 1, "table-2", 1));
    }

    private StateStoreCommitterLogs logsWithTableCommits(Map<String, Integer> commitsByTableId) {
        return () -> commitsByTableId;
    }

}
