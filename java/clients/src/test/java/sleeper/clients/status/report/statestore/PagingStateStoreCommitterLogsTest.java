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

import sleeper.clients.status.report.statestore.PagingStateStoreCommitterLogs.GetLogs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PagingStateStoreCommitterLogsTest {

    private final List<StateStoreCommitterLogEntry> logs = new ArrayList<>();
    private final List<Duration> foundWaits = new ArrayList<>();

    @Test
    void shouldReturnFirstPageWhenFewerRecordsThanLimit() {
        // Given
        addLogs(entryAt(Instant.parse("2024-08-19T09:01:00Z")));

        // When
        List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(2, Duration.ofMinutes(5), neverCheckAge())
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).isEqualTo(logs);
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldNotReturnMoreThanLimitWithoutPaging() {
        // Given
        addLogs(
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                entryAt(Instant.parse("2024-08-19T09:03:00Z")));

        // When
        List<StateStoreCommitterLogEntry> found = getLogsWithLimit(2)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).containsExactly(
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")));
    }

    @Test
    void shouldReturnMoreRecordsThanLimitWhenLogsAreOlderThanMinimumAgeAndOneMorePageAvailable() {
        // Given
        addLogs(
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                entryAt(Instant.parse("2024-08-19T09:03:00Z")));
        Iterator<Instant> checkAge = checkAgeAtTime(
                Instant.parse("2024-08-19T10:30:00Z"));

        // When
        List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(2, Duration.ofMinutes(5), checkAge)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).isEqualTo(logs);
        assertThat(checkAge).isExhausted();
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldReturnMoreRecordsThanLimitWhenLogsAreOlderThanMinimumAgeAndEntriesAtEndOfFirstPageAreAtSameTimeAsStartOfSecondPage() {
        // Given
        addLogs(
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                entryAt(Instant.parse("2024-08-19T09:03:00Z")));
        Iterator<Instant> checkAge = checkAgeAtTime(
                Instant.parse("2024-08-19T10:30:00Z"));

        // When
        List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(3, Duration.ofMinutes(5), checkAge)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).isEqualTo(logs);
        assertThat(checkAge).isExhausted();
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldReturnMoreRecordsThanLimitWhenLogsAreOlderThanMinimumAgeAndEntriesAtEndOfFirstPageAreAtSameTime() {
        // Given
        addLogs(
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                entryAt(Instant.parse("2024-08-19T09:03:00Z")));
        Iterator<Instant> checkAge = checkAgeAtTime(
                Instant.parse("2024-08-19T10:30:00Z"));

        // When
        List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(3, Duration.ofMinutes(5), checkAge)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).isEqualTo(logs);
        assertThat(checkAge).isExhausted();
        assertThat(foundWaits).isEmpty();
    }

    @Test
    void shouldFailToFindNextPageWhenAllLogsInFirstPageAreAtSameTime() {
        // Given
        addLogs(
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                entryAt(Instant.parse("2024-08-19T09:02:00Z")));
        Iterator<Instant> checkAge = checkAgeAtTime(
                Instant.parse("2024-08-19T10:30:00Z"));
        PagingStateStoreCommitterLogs paging = pageAfterLimitAndAgeAtTimes(2, Duration.ofMinutes(5), checkAge);

        // When / Then
        assertThatThrownBy(() -> paging.getLogsInPeriod(
                Instant.parse("2024-08-19T09:00:00Z"),
                Instant.parse("2024-08-19T10:00:00Z")))
                .isInstanceOf(IllegalStateException.class);
    }

    private void addLogs(StateStoreCommitterLogEntry... entries) {
        logs.addAll(List.of(entries));
    }

    private PagingStateStoreCommitterLogs pageAfterLimitAndAgeAtTimes(
            long limit, Duration pagingAge, Iterator<Instant> queryTimes) {
        return new PagingStateStoreCommitterLogs(limit, pagingAge, getLogsWithLimit(limit), queryTimes::next, foundWaits::add);
    }

    private GetLogs getLogsWithLimit(long limit) {
        return (start, end) -> logs.stream()
                .filter(entry -> entry.getTimeInCommitter().equals(start) || entry.getTimeInCommitter().isAfter(start))
                .filter(entry -> entry.getTimeInCommitter().equals(end) || entry.getTimeInCommitter().isBefore(end))
                .limit(limit)
                .collect(toUnmodifiableList());
    }

    private static Iterator<Instant> checkAgeAtTime(Instant time) {
        return List.of(time).iterator();
    }

    private static Iterator<Instant> neverCheckAge() {
        return List.<Instant>of().iterator();
    }

    private StateStoreCommitterLogEntry entryAt(Instant time) {
        return new StateStoreCommitterRunStarted("test-stream", time);
    }

}
