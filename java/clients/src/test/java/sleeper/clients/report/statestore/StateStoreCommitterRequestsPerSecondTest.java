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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.report.statestore.StateStoreCommitterRequestsPerSecond.averageRequestsPerSecondInRunsAndOverall;

public class StateStoreCommitterRequestsPerSecondTest {

    private static final String DEFAULT_LOG_STREAM = "test-stream";
    private static final String DEFAULT_TABLE_ID = "test-table";

    private List<StateStoreCommitterLogEntry> logs = new ArrayList<>();

    @Test
    void shouldFindOneRequestOnOneRunLastingOneSecond() {
        // Given
        runStartedAtTime(Instant.parse("2024-08-15T10:40:00Z"));
        committedAtTime(Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.0, 1.0));
    }

    @Test
    void shouldFindTwoRequestsOnOneRunLastingOneSecond() {
        // Given
        runStartedAtTime(Instant.parse("2024-08-15T10:40:00Z"));
        committedAtTime(Instant.parse("2024-08-15T10:40:00.500Z"));
        committedAtTime(Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(2.0, 2.0));
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
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.0, 2.0));
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
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.5, 2.0));
    }

    @Test
    void shouldFindTwoRequestsOnSeparateRunsWhenFirstRunFinishesAfterLastRun() {
        // Given
        runStartedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:00Z"));
        runStartedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:00.500Z"));
        committedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:01.500Z"));
        runFinishedOnStreamAtTime("stream-2", Instant.parse("2024-08-15T10:40:01.500Z"));
        committedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:02Z"));
        runFinishedOnStreamAtTime("stream-1", Instant.parse("2024-08-15T10:40:02Z"));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(0.75, 1.0));
    }

    @Test
    void shouldIncludeRunWithNoFinishTime() {
        // Given
        runStartedAtTime(Instant.parse("2024-08-15T10:40:00Z"));
        committedAtTime(Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.0, 1.0));
    }

    @Test
    void shouldIgnoreRunWithNoStartTime() {
        // Given
        committedAtTime(Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:40:01Z"));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(0.0, 0.0));
    }

    @Test
    void shouldIgnoreRunWithNoCommits() {
        // Given
        runStartedAtTime(Instant.parse("2024-08-15T10:40:00Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:40:01Z"));
        runStartedAtTime(Instant.parse("2024-08-15T10:40:02Z"));
        committedAtTime(Instant.parse("2024-08-15T10:40:03Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:40:03Z"));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.0, 1.0));
    }

    @Test
    void shouldFindNoLogs() {
        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(0.0, 0.0));
    }

    @Test
    void shouldReportByTable() {
        // Given
        runStartedAtTime(Instant.parse("2024-08-15T10:40:00Z"));
        committedToTableAtTime("table-1", Instant.parse("2024-08-15T10:40:01Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:40:01Z"));
        runStartedAtTime(Instant.parse("2024-08-15T10:41:00Z"));
        committedToTableAtTime("table-2", Instant.parse("2024-08-15T10:41:00.500Z"));
        runFinishedAtTime(Instant.parse("2024-08-15T10:41:00.500Z"));

        // When
        Map<String, StateStoreCommitterRequestsPerSecond> report = reportByTable();

        // Then
        assertThat(report).isEqualTo(Map.of(
                "table-1", averageRequestsPerSecondInRunsAndOverall(1.0, 1.0),
                "table-2", averageRequestsPerSecondInRunsAndOverall(2.0, 2.0)));
    }

    @Test
    void shouldUseEarliestStartTimeFoundInMultiThreadRun() {
        // Given
        Instant earliestStart = Instant.now();
        Instant finish = earliestStart.plusSeconds(3);
        //Thread 1
        batchRunStartedAt(earliestStart.plusSeconds(1));
        committedAtTime(Instant.now());
        batchRunFinishedAt(finish);
        //Thread 2 - Earliest start
        batchRunStartedAt(earliestStart);
        committedAtTime(Instant.now());
        batchRunFinishedAt(finish);
        //Thread 3
        batchRunStartedAt(earliestStart.plusSeconds(2));
        committedAtTime(Instant.now());
        batchRunFinishedAt(finish);

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        //3 commits in 3 seconds = 1per second
        //Only ever 1 run in multi threaded state store
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.0, 1.0));
    }

    @Test
    void shouldUseLatestFinishTimeFoundInMultiThreadRun() {
        // Given
        Instant start = Instant.now();
        Instant lastFinish = start.plusSeconds(3);
        //Thread 1
        batchRunStartedAt(start);
        committedAtTime(Instant.now());
        batchRunFinishedAt(lastFinish.minusSeconds(2));
        //Thread 2 - Earliest start
        batchRunStartedAt(start);
        committedAtTime(Instant.now());
        batchRunFinishedAt(lastFinish);
        //Thread 3
        batchRunStartedAt(start);
        committedAtTime(Instant.now());
        batchRunFinishedAt(lastFinish.minusSeconds(1));

        // When
        StateStoreCommitterRequestsPerSecond report = report();

        // Then
        //3 commits in 3 seconds = 1per second
        //Only ever 1 run in multi threaded state store
        assertThat(report).isEqualTo(
                averageRequestsPerSecondInRunsAndOverall(1.0, 1.0));
    }

    private StateStoreCommitterRequestsPerSecond report() {
        return StateStoreCommitterRequestsPerSecond.fromRuns(
                StateStoreCommitterRuns.findRunsByLogStream(logs));
    }

    private Map<String, StateStoreCommitterRequestsPerSecond> reportByTable() {
        return StateStoreCommitterRequestsPerSecond.byTableIdFromRuns(
                StateStoreCommitterRuns.findRunsByLogStream(logs));
    }

    private void runStartedAtTime(Instant time) {
        runStartedOnStreamAtTime(DEFAULT_LOG_STREAM, time);
    }

    private void runStartedOnStreamAtTime(String logStream, Instant time) {
        add(new StateStoreCommitterLambdaRunStarted(logStream, Instant.MIN, time));
    }

    private void batchRunStartedAt(Instant time) {
        add(new StateStoreCommitterThreadRunStarted(DEFAULT_LOG_STREAM, time, time));
    }

    private void batchRunFinishedAt(Instant time) {
        add(new StateStoreCommitterThreadRunFinished(DEFAULT_LOG_STREAM, time, time));
    }

    private void committedAtTime(Instant time) {
        committedOnStreamAtTime(DEFAULT_LOG_STREAM, time);
    }

    private void committedOnStreamAtTime(String logStream, Instant time) {
        add(new StateStoreCommitSummary(logStream, Instant.MIN, DEFAULT_TABLE_ID, "test-commit", time));
    }

    private void committedToTableAtTime(String tableId, Instant time) {
        add(new StateStoreCommitSummary(DEFAULT_LOG_STREAM, Instant.MIN, tableId, "test-commit", time));
    }

    private void runFinishedAtTime(Instant time) {
        runFinishedOnStreamAtTime(DEFAULT_LOG_STREAM, time);
    }

    private void runFinishedOnStreamAtTime(String logStream, Instant time) {
        add(new StateStoreCommitterLambdaRunFinished(logStream, Instant.MIN, time));
    }

    private void add(StateStoreCommitterLogEntry log) {
        logs.add(log);
    }
}
