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

public class StateStoreCommitterLogIndexRunsTest {

    private List<StateStoreCommitterLogEntry> logs = new ArrayList<>();

    @Test
    void shouldReadFinishedRunWithOneCommit() {
        // Given
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");
        StateStoreCommitSummary committed = committedOnStream("test-stream");
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRun(started, finished, committed));
    }

    @Test
    void shouldReadMultipleRunsOnSameLogStream() {
        // Given
        StateStoreCommitterRunStarted started1 = runStartedOnStream("test-stream");
        StateStoreCommitSummary committed1 = committedOnStream("test-stream");
        StateStoreCommitterRunFinished finished1 = runFinishedOnStream("test-stream");
        StateStoreCommitterRunStarted started2 = runStartedOnStream("test-stream");
        StateStoreCommitSummary committed2 = committedOnStream("test-stream");
        StateStoreCommitterRunFinished finished2 = runFinishedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(
                        finishedRun(started1, finished1, committed1),
                        finishedRun(started2, finished2, committed2));
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

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(
                        finishedRun(started1, finished1, committed1),
                        finishedRun(started2, finished2, committed2));
    }

    @Test
    void shouldReadUnfinishedRunWithNoCommits() {
        // Given
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRun(started));
    }

    @Test
    void shouldReadUnfinishedRunWithOneCommit() {
        // Given
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");
        StateStoreCommitSummary committed = committedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRun(started, committed));
    }

    @Test
    void shouldReadUnfinishedRunWithUnknownStartTimeAndOneCommit() {
        // Given
        StateStoreCommitSummary committed = committedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRunUnknownStart(committed));
    }

    @Test
    void shouldReadFinishedRunWithUnknownStartTimeAndNoCommits() {
        // Given
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRunUnknownStart(finished));
    }

    @Test
    void shouldReadFinishedRunWithUnknownStartTimeAndOneCommit() {
        // Given
        StateStoreCommitSummary committed = committedOnStream("test-stream");
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRunUnknownStart(finished, committed));
    }

    @Test
    void shouldReadFinishedThenStartedRunOnSameStream() {
        // Given
        StateStoreCommitterRunFinished finished = runFinishedOnStream("test-stream");
        StateStoreCommitterRunStarted started = runStartedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(finishedRunUnknownStart(finished), unfinishedRun(started));
    }

    @Test
    void shouldReadTwoStartedRunsOnSameStream() {
        // Given
        StateStoreCommitterRunStarted started1 = runStartedOnStream("test-stream");
        StateStoreCommitterRunStarted started2 = runStartedOnStream("test-stream");

        // When
        StateStoreCommitterLogIndex index = StateStoreCommitterLogIndex.from(logs);

        // Then
        assertThat(index.getRuns())
                .containsExactly(unfinishedRun(started1), unfinishedRun(started2));
    }

    private StateStoreCommitterRunStarted runStartedOnStream(String logStream) {
        return add(new StateStoreCommitterRunStarted(logStream, Instant.now()));
    }

    private StateStoreCommitSummary committedOnStream(String logStream) {
        return add(new StateStoreCommitSummary(logStream, "test-table", "test-commit", Instant.now()));
    }

    private StateStoreCommitterRunFinished runFinishedOnStream(String logStream) {
        return add(new StateStoreCommitterRunFinished(logStream, Instant.now()));
    }

    private <T extends StateStoreCommitterLogEntry> T add(T log) {
        logs.add(log);
        return log;
    }

    private StateStoreCommitterRun unfinishedRun(StateStoreCommitterRunStarted start, StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(start.getLogStream())
                .start(start)
                .commits(List.of(commits))
                .build();
    }

    private StateStoreCommitterRun unfinishedRunUnknownStart(StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(commits[0].getLogStream())
                .commits(List.of(commits))
                .build();
    }

    private StateStoreCommitterRun finishedRun(StateStoreCommitterRunStarted start, StateStoreCommitterRunFinished finish, StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(start.getLogStream())
                .start(start)
                .finish(finish)
                .commits(List.of(commits))
                .build();
    }

    private StateStoreCommitterRun finishedRunUnknownStart(StateStoreCommitterRunFinished finish, StateStoreCommitSummary... commits) {
        return StateStoreCommitterRun.builder()
                .logStream(finish.getLogStream())
                .finish(finish)
                .commits(List.of(commits))
                .build();
    }
}
