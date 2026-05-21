/*
 * Copyright 2022-2026 Crown Copyright
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
    private static final String DEFAULT_LOG_STREAM = "test-stream";

    private List<StateStoreCommitterLogEntry> logs = new ArrayList<>();

    @Nested
    @DisplayName("Lambda state store tests")
    class LambdaTests {
        @Test
        void shouldReadFinishedRunWithOneCommit() {
            // Given
            StateStoreCommitterRunStarted started = runStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary committed = committedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunFinished finished = runFinishedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(started, finished, committed));
        }

        @Test
        void shouldReadMultipleRunsOnSameLogStream() {
            // Given
            StateStoreCommitterRunStarted started1 = runStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary committed1 = committedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunFinished finished1 = runFinishedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunStarted started2 = runStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary committed2 = committedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunFinished finished2 = runFinishedOnStream(DEFAULT_LOG_STREAM);

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
            StateStoreCommitterRunStarted started = runStartedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRun(started));
        }

        @Test
        void shouldReadUnfinishedRunWithOneCommit() {
            // Given
            StateStoreCommitterRunStarted started = runStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary committed = committedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRun(started, committed));
        }

        @Test
        void shouldReadUnfinishedRunWithUnknownStartTimeAndOneCommit() {
            // Given
            StateStoreCommitSummary committed = committedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRunUnknownStart(committed));
        }

        @Test
        void shouldReadFinishedRunWithUnknownStartTimeAndNoCommits() {
            // Given
            StateStoreCommitterRunFinished finished = runFinishedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRunUnknownStart(finished));
        }

        @Test
        void shouldReadFinishedRunWithUnknownStartTimeAndOneCommit() {
            // Given
            StateStoreCommitSummary committed = committedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunFinished finished = runFinishedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRunUnknownStart(finished, committed));
        }

        @Test
        void shouldReadFinishedThenStartedRunOnSameStream() {
            // Given
            StateStoreCommitterRunFinished finished = runFinishedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunStarted started = runStartedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRunUnknownStart(finished), unfinishedRun(started));
        }

        @Test
        void shouldReadTwoStartedRunsOnSameStream() {
            // Given
            StateStoreCommitterRunStarted started1 = runStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterRunStarted started2 = runStartedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(unfinishedRun(started1), unfinishedRun(started2));
        }
    }

    @Nested
    @DisplayName("Multi threaded state store tests")
    class MultiThreadedTests {

        @Test
        void shouldReadLogsFromMultipleThreadsAsOneRun() {
            // Given
            //Table 1 thread logs
            StateStoreCommitterThreadRunStarted started = batchRunStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary thread1 = committedOnStream(DEFAULT_LOG_STREAM);
            batchRunFinishedOnStream(DEFAULT_LOG_STREAM);
            //Table 2 thread logs
            batchRunStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary thread2 = committedOnStream(DEFAULT_LOG_STREAM);
            batchRunFinishedOnStream(DEFAULT_LOG_STREAM);
            //Table 3 thread logs
            batchRunStartedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitSummary thread3 = committedOnStream(DEFAULT_LOG_STREAM);
            StateStoreCommitterThreadRunFinished finished = batchRunFinishedOnStream(DEFAULT_LOG_STREAM);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(started, finished, thread1, thread2, thread3));
        }

        @Test
        void shouldHandleMultipleLogStreamsAsSeperateRuns() {
            // Given
            //Table 1 thread logs
            StateStoreCommitterThreadRunStarted earliest1 = batchRunStartedOnStream("stream-1");
            StateStoreCommitSummary thread1 = committedOnStream("stream-1");
            StateStoreCommitterThreadRunFinished finished1 = batchRunFinishedOnStream("stream-1");
            //Table 2 thread logs
            StateStoreCommitterThreadRunStarted earliest2 = batchRunStartedOnStream("stream-2");
            StateStoreCommitSummary thread2 = committedOnStream("stream-2");
            StateStoreCommitterThreadRunFinished finished2 = batchRunFinishedOnStream("stream-2");

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(earliest1, finished1, thread1),
                            finishedRun(earliest2, finished2, thread2));
        }
    }

    private StateStoreCommitterRunStarted runStartedOnStream(String logStream) {
        return add(new StateStoreCommitterLambdaRunStarted(logStream, Instant.now(), Instant.now()));
    }

    private StateStoreCommitSummary committedOnStream(String logStream) {
        return add(new StateStoreCommitSummary(logStream, Instant.now(), "test-table", "test-commit", Instant.now()));
    }

    private StateStoreCommitterRunFinished runFinishedOnStream(String logStream) {
        return add(new StateStoreCommitterLambdaRunFinished(logStream, Instant.now(), Instant.now()));
    }

    private StateStoreCommitterThreadRunStarted batchRunStartedOnStream(String logStream) {
        return add(new StateStoreCommitterThreadRunStarted(logStream, Instant.now(), Instant.now()));
    }

    private StateStoreCommitterThreadRunFinished batchRunFinishedOnStream(String logStream) {
        return add(new StateStoreCommitterThreadRunFinished(logStream, Instant.now(), Instant.now()));
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
