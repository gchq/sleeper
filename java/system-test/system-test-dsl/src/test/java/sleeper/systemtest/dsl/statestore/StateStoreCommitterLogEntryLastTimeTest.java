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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterLogEntryLastTimeTest {

    @Test
    void shouldGetStartTimeWhenRunJustStarted() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        List<StateStoreCommitterLogEntry> entries = List.of(runStarted(startTime));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(startTime);
    }

    @Test
    void shouldGetFinishTimeWhenRunFinished() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant finishTime = Instant.parse("2024-08-14T09:58:10Z");
        List<StateStoreCommitterLogEntry> entries = List.of(runStarted(startTime), runFinished(finishTime));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(finishTime);
    }

    @Test
    void shouldGetFinishTimeWhenRunFinishedWithACommit() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime = Instant.parse("2024-08-14T09:58:05Z");
        Instant finishTime = Instant.parse("2024-08-14T09:58:10Z");
        List<StateStoreCommitterLogEntry> entries = List.of(
                runStarted(startTime), commitAtTime(commitTime), runFinished(finishTime));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(finishTime);
    }

    @Test
    void shouldGetCommitTimeWhenRunUnfinishedWithACommit() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime = Instant.parse("2024-08-14T09:58:05Z");
        List<StateStoreCommitterLogEntry> entries = List.of(runStarted(startTime), commitAtTime(commitTime));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(commitTime);
    }

    @Test
    void shouldGetLatestRunWhenLastInList() {
        // Given
        Instant runTime1 = Instant.parse("2024-08-14T09:58:00Z");
        Instant runTime2 = Instant.parse("2024-08-14T09:59:00Z");
        List<StateStoreCommitterLogEntry> entries = List.of(runStarted(runTime1), runStarted(runTime2));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(runTime2);
    }

    @Test
    void shouldGetLatestRunWhenFirstInList() {
        // Given
        Instant runTime1 = Instant.parse("2024-08-14T09:58:00Z");
        Instant runTime2 = Instant.parse("2024-08-14T09:59:00Z");
        List<StateStoreCommitterLogEntry> entries = List.of(runStarted(runTime2), runStarted(runTime1));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(runTime2);
    }

    @Test
    void shouldGetLatestCommitWhenLastInList() {
        // Given
        Instant runTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime1 = Instant.parse("2024-08-14T09:58:01Z");
        Instant commitTime2 = Instant.parse("2024-08-14T09:58:02Z");
        List<StateStoreCommitterLogEntry> entries = List.of(
                runStarted(runTime), commitAtTime(commitTime1), commitAtTime(commitTime2));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(commitTime2);
    }

    @Test
    void shouldGetLatestCommitWhenFirstInList() {
        // Given
        Instant runTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime1 = Instant.parse("2024-08-14T09:58:01Z");
        Instant commitTime2 = Instant.parse("2024-08-14T09:58:02Z");
        List<StateStoreCommitterLogEntry> entries = List.of(
                runStarted(runTime), commitAtTime(commitTime2), commitAtTime(commitTime1));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(commitTime2);
    }

    @Test
    void shouldGetCommitTimeWhenRunStartTimeUnknown() {
        // Given
        Instant commitTime = Instant.parse("2024-08-14T09:58:01Z");
        List<StateStoreCommitterLogEntry> entries = List.of(commitAtTime(commitTime));

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries))
                .contains(commitTime);
    }

    @Test
    void shouldGetNoTimeWhenNoRuns() {
        // Given
        List<StateStoreCommitterLogEntry> entries = List.of();

        // When / Then
        assertThat(StateStoreCommitterLogEntry.getLastTime(entries)).isEmpty();
    }

    private StateStoreCommitterRunStarted runStarted(Instant time) {
        return new StateStoreCommitterRunStarted("test-stream", time);
    }

    private StateStoreCommitterRunFinished runFinished(Instant time) {
        return new StateStoreCommitterRunFinished("test-stream", time);
    }

    private StateStoreCommitSummary commitAtTime(Instant time) {
        return new StateStoreCommitSummary("test-stream", "test-table", "test-commit-type", time);
    }
}
