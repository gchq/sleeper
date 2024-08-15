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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterLogIndexTest {

    @Test
    void shouldReadFinishedRunWithOneCommit() {
        // Given
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");
        StateStoreCommitSummary committed = committedOnStream("test-stream");
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");
        List<StateStoreCommitterLogEntry> logs = List.of(started, committed, finished);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRun(started, finished, committed));
    }

    @Test
    void shouldReadOverlappingRunsOnDifferentLogStreams() {
        // Given
        StateStoreCommitterRunStarted started1 = runStartedOnStream("stream-1");
        StateStoreCommitterRunStarted started2 = runStartedOnStream("stream-2");
        StateStoreCommitSummary committed2 = committedOnStream("stream-2");
        StateStoreCommitSummary committed1 = committedOnStream("stream-1");
        StateStoreCommitterRunFinished finished1 = runFinishedOnStream("stream-1");
        StateStoreCommitterRunFinished finished2 = runFinishedOnStream("stream-2");
        List<StateStoreCommitterLogEntry> logs = List.of(started1, started2, committed2, committed1, finished1, finished2);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(
                        finishedRun(started1, finished1, committed1),
                        finishedRun(started2, finished2, committed2));
    }

    @Test
    @Disabled("TODO")
    void shouldReadUnfinishedRunWithNoCommits() {
        // Given
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");
        List<StateStoreCommitterLogEntry> logs = List.of(started);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRun(started));
    }

    @Test
    @Disabled("TODO")
    void shouldReadUnfinishedRunWithOneCommit() {
        // Given
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");
        StateStoreCommitSummary committed = committedOnStream("test-stream");
        List<StateStoreCommitterLogEntry> logs = List.of(started, committed);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRun(started, committed));
    }

    @Test
    @Disabled("TODO")
    void shouldReadUnfinishedRunWithUnknownStartTimeAndOneCommit() {
        // Given
        StateStoreCommitSummary committed = committedOnStream("test-stream");
        List<StateStoreCommitterLogEntry> logs = List.of(committed);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRunUnknownStart(committed));
    }

    @Test
    @Disabled("TODO")
    void shouldReadFinishedRunWithUnknownStartTimeAndNoCommits() {
        // Given
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");
        List<StateStoreCommitterLogEntry> logs = List.of(finished);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRunUnknownStart(finished));
    }

    @Test
    @Disabled("TODO")
    void shouldReadFinishedRunWithUnknownStartTimeAndOneCommit() {
        // Given
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");
        StateStoreCommitSummary committed = committedOnStream("test-stream");
        List<StateStoreCommitterLogEntry> logs = List.of(finished, committed);

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRunUnknownStart(finished, committed));
    }

    private StateStoreCommitterRunStarted runStartedOnStream(String logStream) {
        return new StateStoreCommitterRunStarted(logStream, Instant.now());
    }

    private StateStoreCommitSummary committedOnStream(String logStream) {
        return new StateStoreCommitSummary(logStream, "test-table", "test-commit", Instant.now());
    }

    private StateStoreCommitterRunFinished runFinishedOnStream(String logStream) {
        return new StateStoreCommitterRunFinished(logStream, Instant.now());
    }

    private StateStoreCommitterRun unfinishedRun(StateStoreCommitterRunStarted started, StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(started.getLogStream())
                .startTime(started.getStartTime())
                .commits(List.of(commits))
                .build();
    }

    private StateStoreCommitterRun unfinishedRunUnknownStart(StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(commits[0].getLogStream())
                .commits(List.of(commits))
                .build();
    }

    private StateStoreCommitterRun finishedRun(StateStoreCommitterRunStarted started, StateStoreCommitterRunFinished finished, StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(started.getLogStream())
                .startTime(started.getStartTime())
                .finishTime(finished.getFinishTime())
                .commits(List.of(commits))
                .build();
    }

    private StateStoreCommitterRun finishedRunUnknownStart(StateStoreCommitterRunFinished finished, StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(finished.getLogStream())
                .finishTime(finished.getFinishTime())
                .commits(List.of(commits))
                .build();
    }
}
