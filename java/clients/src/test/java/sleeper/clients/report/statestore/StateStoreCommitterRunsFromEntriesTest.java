/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.report.statestore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StateStoreCommitterRunsFromEntriesTest {

    private List<StateStoreCommitterLogEntry> logs = new ArrayList<>();
    private String testStream = "test-stream";

    @Nested
    @DisplayName("Lambda state store tests")
    class LambdaTests {
        @Test
        void shouldReadFinishedRunWithOneCommit() {
            // Given
            StateStoreCommitterRunStarted started = runStartedOnStream(testStream);
            StateStoreCommitSummary committed = committedOnStream(testStream);
            StateStoreCommitterRunFinished finished = runFinishedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(started, finished, committed));
        }

        @Test
        void shouldReadMultipleRunsOnSameLogStream() {
            // Given
            StateStoreCommitterRunStarted started1 = runStartedOnStream(testStream);
            StateStoreCommitSummary committed1 = committedOnStream(testStream);
            StateStoreCommitterRunFinished finished1 = runFinishedOnStream(testStream);
            StateStoreCommitterRunStarted started2 = runStartedOnStream(testStream);
            StateStoreCommitSummary committed2 = committedOnStream(testStream);
            StateStoreCommitterRunFinished finished2 = runFinishedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
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
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(
                            finishedRun(started1, finished1, committed1),
                            finishedRun(started2, finished2, committed2));
        }

        @Test
        void shouldReadUnfinishedRunWithNoCommits() {
            // Given
            StateStoreCommitterRunStarted started = runStartedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRun(started));
        }

        @Test
        void shouldReadUnfinishedRunWithOneCommit() {
            // Given
            StateStoreCommitterRunStarted started = runStartedOnStream(testStream);
            StateStoreCommitSummary committed = committedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRun(started, committed));
        }

        @Test
        void shouldReadUnfinishedRunWithUnknownStartTimeAndOneCommit() {
            // Given
            StateStoreCommitSummary committed = committedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRunUnknownStart(committed));
        }

        @Test
        void shouldReadFinishedRunWithUnknownStartTimeAndNoCommits() {
            // Given
            StateStoreCommitterRunFinished finished = runFinishedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRunUnknownStart(finished));
        }

        @Test
        void shouldReadFinishedRunWithUnknownStartTimeAndOneCommit() {
            // Given
            StateStoreCommitSummary committed = committedOnStream(testStream);
            StateStoreCommitterRunFinished finished = runFinishedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRunUnknownStart(finished, committed));
        }

        @Test
        void shouldReadFinishedThenStartedRunOnSameStream() {
            // Given
            StateStoreCommitterRunFinished finished = runFinishedOnStream(testStream);
            StateStoreCommitterRunStarted started = runStartedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRunUnknownStart(finished), unfinishedRun(started));
        }

        @Test
        void shouldReadTwoStartedRunsOnSameStream() {
            // Given
            StateStoreCommitterRunStarted started1 = runStartedOnStream(testStream);
            StateStoreCommitterRunStarted started2 = runStartedOnStream(testStream);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRun(started1), unfinishedRun(started2));
        }
    }

    @Nested
    @DisplayName("Multi threaded state sore tests")
    class MultiThreadedTests {

        @Test
        void shouldUseEarliestStartTime() {
            //Given
            batchRunStartedAt(testStream, Instant.parse("2026-01-02T00:00:00Z"));
            StateStoreCommitterRunBatchStarted firstBatch = batchRunStartedAt(testStream, Instant.parse("2026-01-01T00:00:00Z"));
            batchRunStartedAt(testStream, Instant.parse("2026-01-03T00:00:00Z"));

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            //Then
            assertThat(runs.size()).isEqualTo(1);
            assertThat(runs.get(0)).isEqualTo(unfinishedRun(firstBatch));
        }

        @Test
        void shouldUseLatestFinishTime() {
            //Given
            batchRunFinishedAt(testStream, Instant.parse("2026-01-01T00:00:00Z"));
            StateStoreCommitterRunBatchFinished lastBatch = batchRunFinishedAt(testStream, Instant.parse("2026-01-03T00:00:00Z"));
            batchRunFinishedAt(testStream, Instant.parse("2026-01-02T00:00:00Z"));

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            //Then
            assertThat(runs.size()).isEqualTo(1);
            assertThat(runs.get(0)).isEqualTo(finishedRunUnknownStart(lastBatch));
        }

        @Test
        void shouldReadFinishedRunWithOneCommit() {
            // Given
            StateStoreCommitterRunBatchStarted started = batchRunStartedAt(testStream, Instant.now());
            StateStoreCommitSummary committed = committedOnStream(testStream);
            StateStoreCommitterRunBatchFinished finished = batchRunFinishedAt(testStream, Instant.now());

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(started, finished, committed));
        }
    }

    private StateStoreCommitterRunStarted runStartedOnStream(String logStream) {
        return add(new StateStoreCommitterRunStarted(logStream, Instant.now(), Instant.now()));
    }

    private StateStoreCommitSummary committedOnStream(String logStream) {
        return add(new StateStoreCommitSummary(logStream, Instant.now(), "test-table", "test-commit", Instant.now()));
    }

    private StateStoreCommitterRunFinished runFinishedOnStream(String logStream) {
        return add(new StateStoreCommitterRunFinished(logStream, Instant.now(), Instant.now()));
    }

    private StateStoreCommitterRunBatchStarted batchRunStartedAt(String logStream, Instant startTime) {
        return add(new StateStoreCommitterRunBatchStarted(logStream, Instant.now(), startTime));
    }

    private StateStoreCommitterRunBatchFinished batchRunFinishedAt(String logStream, Instant finishTime) {
        return add(new StateStoreCommitterRunBatchFinished(logStream, Instant.now(), finishTime));
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
