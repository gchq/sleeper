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
package sleeper.clients.status.report.statestore;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterRunsByTableTest {
    private static final String DEFAULT_LOG_STREAM = "test-stream";

    private final List<StateStoreCommitterRun> runs = new ArrayList<>();

    @Test
    void shouldFindRunsOnDifferentTables() {
        // Given
        StateStoreCommitterRun run1 = finishedRunWithCommitsInTables("table-1");
        StateStoreCommitterRun run2 = finishedRunWithCommitsInTables("table-2");

        // When
        Map<String, List<StateStoreCommitterRun>> runsByTableId = indexRunsByTableId();

        // Then
        assertThat(runsByTableId).isEqualTo(Map.of(
                "table-1", List.of(run1),
                "table-2", List.of(run2)));
    }

    @Test
    void shouldFindRunWithMultipleTables() {
        // Given
        StateStoreCommitterRun run = finishedRunWithCommitsInTables("table-1", "table-2");

        // When
        Map<String, List<StateStoreCommitterRun>> runsByTableId = indexRunsByTableId();

        // Then
        assertThat(runsByTableId).isEqualTo(Map.of(
                "table-1", List.of(run),
                "table-2", List.of(run)));
    }

    @Test
    void shouldFindRunWithSameTableMultipleTimes() {
        // Given
        StateStoreCommitterRun run = finishedRunWithCommitsInTables("test-table", "test-table");

        // When
        Map<String, List<StateStoreCommitterRun>> runsByTableId = indexRunsByTableId();

        // Then
        assertThat(runsByTableId).isEqualTo(Map.of(
                "test-table", List.of(run)));
    }

    @Test
    void shouldFindTableWithMultipleRuns() {
        // Given
        StateStoreCommitterRun run1 = finishedRunWithCommitsInTables("test-table");
        StateStoreCommitterRun run2 = finishedRunWithCommitsInTables("test-table");

        // When
        Map<String, List<StateStoreCommitterRun>> runsByTableId = indexRunsByTableId();

        // Then
        assertThat(runsByTableId).isEqualTo(Map.of(
                "test-table", List.of(run1, run2)));
    }

    private Map<String, List<StateStoreCommitterRun>> indexRunsByTableId() {
        return StateStoreCommitterRuns.indexRunsByTableId(runs);
    }

    private StateStoreCommitterRun finishedRunWithCommitsInTables(String... commitTableIds) {
        return add(StateStoreCommitterRun.builder()
                .logStream(DEFAULT_LOG_STREAM)
                .start(new StateStoreCommitterRunStarted(DEFAULT_LOG_STREAM, Instant.now()))
                .finish(new StateStoreCommitterRunFinished(DEFAULT_LOG_STREAM, Instant.now()))
                .commits(commitsWithTableIds(commitTableIds))
                .build());
    }

    private List<StateStoreCommitSummary> commitsWithTableIds(String... tableIds) {
        return Stream.of(tableIds)
                .map(tableId -> new StateStoreCommitSummary(DEFAULT_LOG_STREAM, tableId, "test-commit", Instant.now()))
                .collect(toUnmodifiableList());
    }

    private StateStoreCommitterRun add(StateStoreCommitterRun run) {
        runs.add(run);
        return run;
    }

}
