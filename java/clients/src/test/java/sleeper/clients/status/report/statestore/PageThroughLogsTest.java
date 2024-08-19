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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.clients.status.report.statestore.PageThroughLogs.GetLogs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PageThroughLogsTest {

    private final List<StateStoreCommitterLogEntry> logs = new ArrayList<>();
    private final List<List<StateStoreCommitterLogEntry>> foundPages = new ArrayList<>();
    private final List<Duration> foundWaits = new ArrayList<>();

    @Nested
    @DisplayName("Page through records based on timestamp of last entry")
    class PageByTimestamp {

        @Test
        void shouldReturnFirstPageWhenFewerRecordsThanLimit() {
            // Given
            addLogs(entryAt(Instant.parse("2024-08-19T09:01:00Z")));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimit(3)
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
        void shouldReturnMoreRecordsThanLimitWhenOneMorePageAvailable() {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:04:00Z")));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimit(3)
                    .getLogsInPeriod(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"));

            // Then
            assertThat(found).isEqualTo(logs);
            assertThat(foundWaits).isEmpty();
        }

        @Test
        void shouldReturnMoreRecordsThanLimitWhenEntriesAtEndOfFirstPageAreAtSameTimeAsStartOfSecondPage() {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimit(3)
                    .getLogsInPeriod(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"));

            // Then
            assertThat(found).isEqualTo(logs);
            assertThat(foundWaits).isEmpty();
        }

        @Test
        void shouldReturnMoreRecordsThanLimitWhenEntriesAtEndOfFirstPageAreAtSameTime() {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimit(3)
                    .getLogsInPeriod(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"));

            // Then
            assertThat(found).isEqualTo(logs);
            assertThat(foundWaits).isEmpty();
        }

        @Test
        void shouldFailToFindNextPageWhenAllLogsInFirstPageAreAtSameTime() {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")));
            PageThroughLogs<StateStoreCommitterLogEntry> paging = pageAfterLimit(2);

            // When / Then
            assertThatThrownBy(() -> paging.getLogsInPeriod(
                    Instant.parse("2024-08-19T09:00:00Z"),
                    Instant.parse("2024-08-19T10:00:00Z")))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    @DisplayName("Apply minimum age for paging")
    class MinimumPagingAge {

        @Test
        void shouldWaitUntilRecordsFoundSoFarMeetMinimumAgeBeforeCheckingAgainAndPaging() {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:04:00Z")));
            Iterator<Instant> checkAge = checkAgeAtTimes(
                    Instant.parse("2024-08-19T09:05:00Z"),
                    Instant.parse("2024-08-19T09:08:00Z"));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(3, Duration.ofMinutes(5), checkAge)
                    .getLogsInPeriod(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"));

            // Then
            assertThat(found).isEqualTo(logs);
            assertThat(checkAge).isExhausted();
            assertThat(foundWaits).containsExactly(Duration.ofMinutes(3));
            assertThat(foundPages).containsExactly(
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:04:00Z"))));
        }

        @Test
        void shouldWaitUntilSomeRecordsFoundSoFarMeetMinimumAgeBeforeCheckingAgainAndPaging() {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:04:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:05:00Z")));
            Iterator<Instant> checkAge = checkAgeAtTimes(
                    Instant.parse("2024-08-19T09:08:00Z"));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(4, Duration.ofMinutes(5), checkAge)
                    .getLogsInPeriod(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"));

            // Then
            assertThat(found).isEqualTo(logs);
            assertThat(checkAge).isExhausted();
            assertThat(foundWaits).containsExactly(Duration.ofMinutes(1));
            assertThat(foundPages).containsExactly(
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:04:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:04:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:05:00Z"))));
        }
    }

    private void addLogs(StateStoreCommitterLogEntry... entries) {
        logs.addAll(List.of(entries));
    }

    private PageThroughLogs<StateStoreCommitterLogEntry> pageAfterLimit(long limit) {
        return new PageThroughLogs<>(limit, Duration.ZERO, getLogsWithLimit(limit), () -> Instant.MAX,
                waitDuration -> new IllegalStateException("Unexpected wait of duration: " + waitDuration));
    }

    private PageThroughLogs<StateStoreCommitterLogEntry> pageAfterLimitAndAgeAtTimes(
            long limit, Duration pagingAge, Iterator<Instant> queryTimes) {
        return new PageThroughLogs<>(limit, pagingAge, getLogsWithLimit(limit), queryTimes::next, foundWaits::add);
    }

    private GetLogs<StateStoreCommitterLogEntry> getLogsWithLimit(long limit) {
        return (start, end) -> {
            List<StateStoreCommitterLogEntry> page = logs.stream()
                    .filter(entry -> entry.getTimestamp().equals(start) || entry.getTimestamp().isAfter(start))
                    .filter(entry -> entry.getTimestamp().equals(end) || entry.getTimestamp().isBefore(end))
                    .limit(limit)
                    .collect(toUnmodifiableList());
            foundPages.add(page);
            return page;
        };
    }

    private static Iterator<Instant> checkAgeAtTimes(Instant... times) {
        return List.of(times).iterator();
    }

    private StateStoreCommitterLogEntry entryAt(Instant time) {
        return new StateStoreCommitterRunStarted("test-stream", time, Instant.MIN);
    }

}
