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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class WaitForStateStoreCommitsTest {
    Map<String, Integer> waitForNumCommitsByTableId = new HashMap<>();

    @Test
    void shouldFindOneCommitWasMadeAgainstCorrectTable() {
        // Given
        waitForNumCommitsByTableId.put("test-table", 2);

        // When
        WaitForStateStoreCommits.decrementWaitForNumCommits(
                List.of(commitToTable("test-table")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEqualTo(Map.of("test-table", 1));
    }

    @Test
    void shouldFindAllCommitsWereMadeAgainstCorrectTable() {
        // Given
        waitForNumCommitsByTableId.put("test-table", 2);

        // When
        WaitForStateStoreCommits.decrementWaitForNumCommits(
                List.of(commitToTable("test-table"), commitToTable("test-table")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEmpty();
    }

    @Test
    void shouldFindCommitAgainstWrongTable() {
        // Given
        waitForNumCommitsByTableId.put("test-table", 2);

        // When
        WaitForStateStoreCommits.decrementWaitForNumCommits(
                List.of(commitToTable("other-table")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEqualTo(Map.of("test-table", 2));
    }

    @Test
    void shouldFindCommitsAgainstMultipleTablesInOneRun() {
        // Given
        waitForNumCommitsByTableId.put("table-1", 2);
        waitForNumCommitsByTableId.put("table-2", 2);

        // When
        WaitForStateStoreCommits.decrementWaitForNumCommits(
                List.of(commitToTable("table-1"), commitToTable("table-2")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEqualTo(Map.of("table-1", 1, "table-2", 1));
    }

    @Test
    void shouldFindCommitsAgainstMultipleTablesInSeparateRuns() {
        // Given
        waitForNumCommitsByTableId.put("table-1", 2);
        waitForNumCommitsByTableId.put("table-2", 2);

        // When
        WaitForStateStoreCommits.decrementWaitForNumCommits(List.of(
                commitToTable("table-1"),
                commitToTable("table-2")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEqualTo(Map.of("table-1", 1, "table-2", 1));
    }

    private StateStoreCommitSummary commitToTable(String tableId) {
        return new StateStoreCommitSummary("test-stream", tableId, "test-commit-type", Instant.now());
    }

}
