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

public class StateStoreCommitterRunLastTimeTest {

    @Test
    void shouldGetStartTimeWhenRunJustStarted() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        List<StateStoreCommitterRun> runs = List.of(unfinishedRun(startTime));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(startTime);
    }

    @Test
    void shouldGetFinishTimeWhenRunFinished() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant finishTime = Instant.parse("2024-08-14T09:58:10Z");
        List<StateStoreCommitterRun> runs = List.of(finishedRun(startTime, finishTime));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(finishTime);
    }

    @Test
    void shouldGetFinishTimeWhenRunFinishedWithACommit() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime = Instant.parse("2024-08-14T09:58:05Z");
        Instant finishTime = Instant.parse("2024-08-14T09:58:10Z");
        List<StateStoreCommitterRun> runs = List.of(finishedRun(startTime, finishTime, commitAtTime(commitTime)));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(finishTime);
    }

    @Test
    void shouldGetCommitTimeWhenRunUnfinishedWithACommit() {
        // Given
        Instant startTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime = Instant.parse("2024-08-14T09:58:05Z");
        List<StateStoreCommitterRun> runs = List.of(unfinishedRun(startTime, commitAtTime(commitTime)));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(commitTime);
    }

    @Test
    void shouldGetLatestRunWhenLastInList() {
        // Given
        Instant runTime1 = Instant.parse("2024-08-14T09:58:00Z");
        Instant runTime2 = Instant.parse("2024-08-14T09:59:00Z");
        List<StateStoreCommitterRun> runs = List.of(unfinishedRun(runTime1), unfinishedRun(runTime2));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(runTime2);
    }

    @Test
    void shouldGetLatestRunWhenFirstInList() {
        // Given
        Instant runTime1 = Instant.parse("2024-08-14T09:58:00Z");
        Instant runTime2 = Instant.parse("2024-08-14T09:59:00Z");
        List<StateStoreCommitterRun> runs = List.of(unfinishedRun(runTime2), unfinishedRun(runTime1));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(runTime2);
    }

    @Test
    void shouldGetLatestCommitWhenLastInList() {
        // Given
        Instant runTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime1 = Instant.parse("2024-08-14T09:58:01Z");
        Instant commitTime2 = Instant.parse("2024-08-14T09:58:02Z");
        List<StateStoreCommitterRun> runs = List.of(
                unfinishedRun(runTime, commitAtTime(commitTime1), commitAtTime(commitTime2)));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(commitTime2);
    }

    @Test
    void shouldGetLatestCommitWhenFirstInList() {
        // Given
        Instant runTime = Instant.parse("2024-08-14T09:58:00Z");
        Instant commitTime1 = Instant.parse("2024-08-14T09:58:01Z");
        Instant commitTime2 = Instant.parse("2024-08-14T09:58:02Z");
        List<StateStoreCommitterRun> runs = List.of(
                unfinishedRun(runTime, commitAtTime(commitTime2), commitAtTime(commitTime1)));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(commitTime2);
    }

    @Test
    void shouldGetCommitTimeWhenRunStartTimeUnknown() {
        // Given
        Instant commitTime = Instant.parse("2024-08-14T09:58:01Z");
        List<StateStoreCommitterRun> runs = List.of(
                unknownStartUnfinishedRun(commitAtTime(commitTime)));

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs))
                .contains(commitTime);
    }

    @Test
    void shouldGetNoTimeWhenNoRuns() {
        // Given
        List<StateStoreCommitterRun> runs = List.of();

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs)).isEmpty();
    }

    @Test
    void shouldGetNoTimeWhenRunWithUnknownTime() {
        // Given
        List<StateStoreCommitterRun> runs = List.of(unknownStartUnfinishedRun());

        // When / Then
        assertThat(StateStoreCommitterRun.getLastTime(runs)).isEmpty();
    }

    private StateStoreCommitterRun unfinishedRun(Instant startTime, StateStoreCommitSummary... commits) {
        return new StateStoreCommitterRun(startTime, null, List.of(commits));
    }

    private StateStoreCommitterRun finishedRun(Instant startTime, Instant finishTime, StateStoreCommitSummary... commits) {
        return new StateStoreCommitterRun(startTime, finishTime, List.of(commits));
    }

    private StateStoreCommitterRun unknownStartUnfinishedRun(StateStoreCommitSummary... commits) {
        return new StateStoreCommitterRun(null, null, List.of(commits));
    }

    private StateStoreCommitSummary commitAtTime(Instant time) {
        return new StateStoreCommitSummary("test-table", "test-commit-type", time);
    }
}
