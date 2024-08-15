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

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterLogIndexThroughputTest {

    private List<StateStoreCommitterLogEntry> logs = new ArrayList<>();

    @Test
    void shouldFindOneRequestOnOneRunLastingOneSecond() {
        // Given
        runStartedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:00Z"));
        committedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getAverageRequestsPerSecondInRuns()).isEqualTo(1.0);
        assertThat(index.getAverageRequestsPerSecondOverall()).isEqualTo(1.0);
    }

    @Test
    void shouldFindTwoRequestsOnOneRunLastingOneSecond() {
        // Given
        runStartedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:00Z"));
        committedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:00.500Z"));
        committedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedOnStreamAtTime("test-stream", Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getAverageRequestsPerSecondInRuns()).isEqualTo(2.0);
        assertThat(index.getAverageRequestsPerSecondOverall()).isEqualTo(2.0);
    }

    @Test
    void shouldFindTwoRequestsOnSeparateRunsLastingOneSecond() {
        // Given
        runStartedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:00Z"));
        runStartedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:00Z"));
        committedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:01Z"));
        committedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getAverageRequestsPerSecondInRuns()).isEqualTo(1.0);
        assertThat(index.getAverageRequestsPerSecondOverall()).isEqualTo(2.0);
    }

    @Test
    void shouldFindTwoRequestsOnSeparateRunsLastingDifferentTimes() {
        // Given
        runStartedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:00Z"));
        runStartedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:00Z"));
        committedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:00.500Z"));
        runFinishedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:00.500Z"));
        committedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getAverageRequestsPerSecondInRuns()).isEqualTo(1.5);
        assertThat(index.getAverageRequestsPerSecondOverall()).isEqualTo(2.0);
    }

    private StateStoreCommitterRunStarted runStartedOnStreamAtTime(String logStream, Instant time) {
        return add(new StateStoreCommitterRunStarted(logStream, time));
    }

    private StateStoreCommitSummary committedOnStreamAtTime(String logStream, Instant time) {
        return add(new StateStoreCommitSummary(logStream, "test-table", "test-commit", time));
    }

    private StateStoreCommitterRunFinished runFinishedOnStreamAtTime(String logStream, Instant time) {
        return add(new StateStoreCommitterRunFinished(logStream, time));
    }

    private <T extends StateStoreCommitterLogEntry> T add(T log) {
        logs.add(log);
        return log;
    }
}
