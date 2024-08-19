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

import sleeper.clients.status.report.statestore.PagingStateStoreCommitterLogs.GetLogs;

import java.time.Instant;
import java.util.List;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.Assertions.assertThat;

public class PagingStateStoreCommitterLogsTest {

    @Test
    void shouldReturnFirstPageWhenFewerRecordsThanLimit() {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                entryAt(Instant.parse("2024-08-19T09:04:00Z")));

        // When
        List<StateStoreCommitterLogEntry> found = pageAfterLimit(2, logs)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).isEqualTo(logs);
    }

    @Test
    void shouldNotReturnMoreThanLimitWithoutPaging() {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                entryAt(Instant.parse("2024-08-19T09:04:00Z")),
                entryAt(Instant.parse("2024-08-19T09:05:00Z")));

        // When
        List<StateStoreCommitterLogEntry> found = getLogsWithLimit(1, logs)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).containsExactly(
                entryAt(Instant.parse("2024-08-19T09:04:00Z")));
    }

    @Test
    @Disabled("TODO")
    void shouldReturnAllRecordsWhenMoreRecordsThanLimit() {
        // Given
        List<StateStoreCommitterLogEntry> logs = List.of(
                entryAt(Instant.parse("2024-08-19T09:04:00Z")),
                entryAt(Instant.parse("2024-08-19T09:04:30Z")));

        // When
        List<StateStoreCommitterLogEntry> found = pageAfterLimit(1, logs)
                .getLogsInPeriod(
                        Instant.parse("2024-08-19T09:00:00Z"),
                        Instant.parse("2024-08-19T10:00:00Z"));

        // Then
        assertThat(found).isEqualTo(logs);
    }

    private static PagingStateStoreCommitterLogs pageAfterLimit(long limit, List<StateStoreCommitterLogEntry> logs) {
        return PagingStateStoreCommitterLogs.pageAfterLimit(limit, getLogsWithLimit(limit, logs));
    }

    private static GetLogs getLogsWithLimit(long limit, List<StateStoreCommitterLogEntry> logs) {
        return (start, end) -> logs.stream()
                .filter(entry -> entry.getTimeInCommitter().isAfter(start))
                .filter(entry -> entry.getTimeInCommitter().isBefore(end))
                .limit(limit)
                .collect(toUnmodifiableList());
    }

    private StateStoreCommitterLogEntry entryAt(Instant time) {
        return new StateStoreCommitterRunStarted("test-stream", time);
    }

}
