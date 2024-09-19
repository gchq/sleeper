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
import java.time.temporal.ChronoUnit;
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
        void shouldReturnFirstPageWhenFewerRecordsThanLimit() throws Exception {
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
        void shouldNotReturnMoreThanLimitWithoutPaging() throws Exception {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")));

            // When
            List<StateStoreCommitterLogEntry> found = getLogs()
                    .getLogsInPeriodWithLimit(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"), 2);

            // Then
            assertThat(found).containsExactly(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")));
        }

        @Test
        void shouldReturnMoreRecordsThanLimitWhenOneMorePageAvailable() throws Exception {
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
        void shouldReturnMoreRecordsThanLimitWhenEntriesAtEndOfFirstPageAreAtSameTimeAsStartOfSecondPage() throws Exception {
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
            assertThat(foundPages).containsExactly(
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:03:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))));
        }

        @Test
        void shouldReturnMoreRecordsThanLimitWhenEntriesAtEndOfFirstPageAreAtSameTime() throws Exception {
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
            assertThat(foundPages).containsExactly(
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))));
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
        void shouldWaitUntilRecordsFoundSoFarMeetMinimumAgeBeforeCheckingAgainAndPaging() throws Exception {
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
        void shouldWaitUntilSomeRecordsFoundSoFarMeetMinimumAgeBeforeCheckingAgainAndPaging() throws Exception {
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

    @Nested
    @DisplayName("Query second by second")
    class SecondBySecond {

        @Test
        void shouldTruncateToSecondWhenQueryingWithoutPaging() throws Exception {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:00:59.999Z")),
                    entryAt(Instant.parse("2024-08-19T09:01:00.500Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00.500Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:01.000Z")));

            // When
            List<StateStoreCommitterLogEntry> found = getLogs()
                    .getLogsInPeriodWithLimit(
                            Instant.parse("2024-08-19T09:01:00.750Z"),
                            Instant.parse("2024-08-19T09:03:00.250Z"),
                            10);

            // Then
            assertThat(found).containsExactly(
                    entryAt(Instant.parse("2024-08-19T09:01:00.500Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00.500Z")));
        }

        @Test
        void shouldReturnMoreRecordsThanLimitWhenEntriesAtEndOfFirstPageAreInSameSecond() throws Exception {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00.001Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00.999Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00Z")));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimit(3)
                    .getLogsInPeriod(
                            Instant.parse("2024-08-19T09:00:00Z"),
                            Instant.parse("2024-08-19T10:00:00Z"));

            // Then
            assertThat(found).isEqualTo(logs);
            assertThat(foundWaits).isEmpty();
            assertThat(foundPages).containsExactly(
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00.001Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00.999Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:02:00.001Z")),
                            entryAt(Instant.parse("2024-08-19T09:02:00.999Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:03:00Z"))));
        }

        @Test
        void shouldWaitUntilSomeRecordsFoundSoFarMeetMinimumAgeBeforeCheckingAgainAndPagingWhenEntriesInSameSecond() throws Exception {
            // Given
            addLogs(
                    entryAt(Instant.parse("2024-08-19T09:01:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:02:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00.001Z")),
                    entryAt(Instant.parse("2024-08-19T09:03:00.999Z")),
                    entryAt(Instant.parse("2024-08-19T09:04:00Z")),
                    entryAt(Instant.parse("2024-08-19T09:05:00Z")));
            Iterator<Instant> checkAge = checkAgeAtTimes(
                    Instant.parse("2024-08-19T09:08:00Z"));

            // When
            List<StateStoreCommitterLogEntry> found = pageAfterLimitAndAgeAtTimes(5, Duration.ofMinutes(5), checkAge)
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
                            entryAt(Instant.parse("2024-08-19T09:03:00.001Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00.999Z")),
                            entryAt(Instant.parse("2024-08-19T09:04:00Z"))),
                    List.of(
                            entryAt(Instant.parse("2024-08-19T09:03:00.001Z")),
                            entryAt(Instant.parse("2024-08-19T09:03:00.999Z")),
                            entryAt(Instant.parse("2024-08-19T09:04:00Z")),
                            entryAt(Instant.parse("2024-08-19T09:05:00Z"))));
        }
    }

    private void addLogs(StateStoreCommitterLogEntry... entries) {
        logs.addAll(List.of(entries));
    }

    private PageThroughLogs<StateStoreCommitterLogEntry> pageAfterLimit(int limit) {
        return new PageThroughLogs<>(limit, Duration.ZERO, getLogs(), () -> Instant.MAX,
                waitDuration -> new IllegalStateException("Unexpected wait of duration: " + waitDuration));
    }

    private PageThroughLogs<StateStoreCommitterLogEntry> pageAfterLimitAndAgeAtTimes(
            int limit, Duration pagingAge, Iterator<Instant> queryTimes) {
        return new PageThroughLogs<>(limit, pagingAge, getLogs(), queryTimes::next, foundWaits::add);
    }

    private GetLogs<StateStoreCommitterLogEntry> getLogs() {
        return (rawStart, rawEnd, limit) -> {
            Instant start = rawStart.truncatedTo(ChronoUnit.SECONDS);
            Instant end = rawEnd.truncatedTo(ChronoUnit.SECONDS);
            List<StateStoreCommitterLogEntry> page = logs.stream()
                    .filter(entry -> {
                        Instant timestamp = entry.getTimestamp().truncatedTo(ChronoUnit.SECONDS);
                        return (timestamp.equals(start) || timestamp.isAfter(start))
                                && (timestamp.equals(end) || timestamp.isBefore(end));
                    })
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
