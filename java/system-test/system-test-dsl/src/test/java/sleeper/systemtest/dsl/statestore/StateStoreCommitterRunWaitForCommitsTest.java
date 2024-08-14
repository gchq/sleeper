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
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterRunWaitForCommitsTest {
    Map<String, Integer> waitForNumCommitsByTableId = new HashMap<>();

    @Test
    void shouldFindOneCommitWasMadeAgainstCorrectTable() {
        // Given
        waitForNumCommitsByTableId.put("test-table", 2);

        // When
        StateStoreCommitterRun.decrementWaitForNumCommits(
                List.of(runWithCommitsToTable(1, "test-table")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEqualTo(Map.of("test-table", 1));
    }

    @Test
    void shouldFindAllCommitsWereMadeAgainstCorrectTable() {
        // Given
        waitForNumCommitsByTableId.put("test-table", 2);

        // When
        StateStoreCommitterRun.decrementWaitForNumCommits(
                List.of(runWithCommitsToTable(2, "test-table")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEmpty();
    }

    @Test
    void shouldFindCommitAgainstWrongTable() {
        // Given
        waitForNumCommitsByTableId.put("test-table", 2);

        // When
        StateStoreCommitterRun.decrementWaitForNumCommits(
                List.of(runWithCommitsToTable(1, "other-table")),
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
        StateStoreCommitterRun.decrementWaitForNumCommits(
                List.of(runWithCommits(List.of(commitToTable("table-1"), commitToTable("table-2")))),
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
        StateStoreCommitterRun.decrementWaitForNumCommits(List.of(
                runWithCommitsToTable(1, "table-1"),
                runWithCommitsToTable(1, "table-2")),
                waitForNumCommitsByTableId);

        // Then
        assertThat(waitForNumCommitsByTableId).isEqualTo(Map.of("table-1", 1, "table-2", 1));
    }

    private StateStoreCommitterRun runWithCommitsToTable(int commits, String tableId) {
        return runWithCommits(IntStream.range(0, commits)
                .mapToObj(i -> new StateStoreCommitSummary(tableId, "test-commit-type", Instant.now()))
                .collect(toUnmodifiableList()));
    }

    private StateStoreCommitterRun runWithCommits(List<StateStoreCommitSummary> commits) {
        return new StateStoreCommitterRun(Instant.now(), Instant.now(), commits);
    }

    private StateStoreCommitSummary commitToTable(String tableId) {
        return new StateStoreCommitSummary(tableId, "test-commit-type", Instant.now());
    }

}
