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
        void shouldReadLogsFromMultipleThreadsAsOneRun() {
            // Given
            //Thread 1
            StateStoreCommitterThreadRunStarted started = batchRunStartedAt(testStream, Instant.now());
            StateStoreCommitSummary thread1 = committedOnStream(testStream);
            batchRunFinishedAt(testStream, Instant.now());
            //Thread 2
            batchRunStartedAt(testStream, Instant.now());
            StateStoreCommitSummary thread2 = committedOnStream(testStream);
            batchRunFinishedAt(testStream, Instant.now());
            //Thread 3
            batchRunStartedAt(testStream, Instant.now());
            StateStoreCommitSummary thread3 = committedOnStream(testStream);
            StateStoreCommitterThreadRunFinished finished = batchRunFinishedAt(testStream, Instant.now());

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(started, finished, thread1, thread2, thread3));
        }

        @Test
        void shouldReadEarliestStartTimeFromMultipleBatchStarts() {
            // Given
            Instant earliestStart = Instant.now();
            Instant finish = earliestStart.plusSeconds(3);
            //Thread 1
            batchRunStartedAt(testStream, earliestStart.plusSeconds(1));
            StateStoreCommitSummary thread1 = committedOnStream(testStream);
            StateStoreCommitterThreadRunFinished finished = batchRunFinishedAt(testStream, finish);
            //Thread 2
            StateStoreCommitterThreadRunStarted earliest = batchRunStartedAt(testStream, earliestStart);
            StateStoreCommitSummary thread2 = committedOnStream(testStream);
            batchRunFinishedAt(testStream, finish);
            //Thread 3
            batchRunStartedAt(testStream, earliestStart.plusSeconds(2));
            StateStoreCommitSummary thread3 = committedOnStream(testStream);
            batchRunFinishedAt(testStream, finish);

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(earliest, finished, thread1, thread2, thread3));
        }

        @Test
        void shouldReadLatestFinishTimeFromMultipleBatchStarts() {
            // Given
            Instant start = Instant.now();
            Instant latestFinish = start.plusSeconds(3);
            //Thread 1
            StateStoreCommitterThreadRunStarted earliest = batchRunStartedAt(testStream, start);
            StateStoreCommitSummary thread1 = committedOnStream(testStream);
            batchRunFinishedAt(testStream, latestFinish.minusSeconds(2));
            //Thread 2
            batchRunStartedAt(testStream, start);
            StateStoreCommitSummary thread2 = committedOnStream(testStream);
            StateStoreCommitterThreadRunFinished finished = batchRunFinishedAt(testStream, latestFinish);
            //Thread 3
            batchRunStartedAt(testStream, start);
            StateStoreCommitSummary thread3 = committedOnStream(testStream);
            batchRunFinishedAt(testStream, latestFinish.minusSeconds(1));

            // When
            List<StateStoreCommitterRun> runs = StateStoreCommitterRuns.findRunsByLogStream(logs);

            // Then
            assertThat(runs)
                    .containsExactly(finishedRun(earliest, finished, thread1, thread2, thread3));
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

    private StateStoreCommitterThreadRunStarted batchRunStartedAt(String logStream, Instant startTime) {
        return add(new StateStoreCommitterThreadRunStarted(logStream, Instant.now(), startTime));
    }

    private StateStoreCommitterThreadRunFinished batchRunFinishedAt(String logStream, Instant finishTime) {
        return add(new StateStoreCommitterThreadRunFinished(logStream, Instant.now(), finishTime));
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
