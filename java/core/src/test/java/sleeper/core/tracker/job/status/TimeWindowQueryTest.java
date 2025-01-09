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

package sleeper.core.tracker.job.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeWindowQueryTest {
    @Nested
    @DisplayName("Job started")
    class JobStarted {
        @Test
        void shouldBeInPeriodWhereStartTimeIsBeforePeriodStartTime() {
            Instant startTime = Instant.parse("2023-08-16T11:00:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenStartTimeIsAfterPeriodStartTimeButBeforePeriodEndTime() {
            Instant startTime = Instant.parse("2023-08-16T12:30:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isTrue();
        }

        @Test
        void shouldNotBeInPeriodWhenStartTimeIsAfterPeriodEndTime() {
            Instant startTime = Instant.parse("2023-08-16T14:00:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("Job finished")
    class JobFinished {
        @Test
        void shouldBeInPeriodWhenJobStartsAndFinishesInsidePeriod() {
            Instant startTime = Instant.parse("2023-08-16T12:20:00Z");
            Instant endTime = Instant.parse("2023-08-16T12:40:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isFinishedJobInWindow(startTime, endTime))
                    .isTrue();
        }

        @Test
        void shouldNotBeInPeriodWhenJobFinishesBeforePeriod() {
            Instant startTime = Instant.parse("2023-08-16T11:20:00Z");
            Instant endTime = Instant.parse("2023-08-16T11:40:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isFinishedJobInWindow(startTime, endTime))
                    .isFalse();
        }

        @Test
        void shouldNotBeInPeriodWhenJobStartsAfterPeriod() {
            Instant startTime = Instant.parse("2023-08-16T13:20:00Z");
            Instant endTime = Instant.parse("2023-08-16T13:40:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isFinishedJobInWindow(startTime, endTime))
                    .isFalse();
        }

        @Test
        void shouldBeInPeriodWhenJobOverlapsEndOfPeriod() {
            Instant startTime = Instant.parse("2023-08-16T12:40:00Z");
            Instant endTime = Instant.parse("2023-08-16T13:20:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isFinishedJobInWindow(startTime, endTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenJobOverlapsStartOfPeriod() {
            Instant startTime = Instant.parse("2023-08-16T11:40:00Z");
            Instant endTime = Instant.parse("2023-08-16T12:20:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isFinishedJobInWindow(startTime, endTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenJobContainsWindowPeriod() {
            Instant startTime = Instant.parse("2023-08-16T11:00:00Z");
            Instant endTime = Instant.parse("2023-08-16T14:00:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"));

            assertThat(timeWindowQuery.isFinishedJobInWindow(startTime, endTime))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("Job run time can be limited")
    class JobRuntimeLimited {

        @Test
        void shouldNotBeInPeriodWhenMaxRuntimeIsMetBeforeWindow() {
            Instant startTime = Instant.parse("2023-08-16T10:00:00Z");
            Duration maxRuntime = Duration.ofHours(1);
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"),
                    maxRuntime);

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isFalse();
        }

        @Test
        void shouldBeInPeriodWhenMaxRuntimeIsMetDuringWindow() {
            Instant startTime = Instant.parse("2023-08-16T11:30:00Z");
            Duration maxRuntime = Duration.ofHours(1);
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"),
                    maxRuntime);

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenMaxRuntimeIsMetAfterWindow() {
            Instant startTime = Instant.parse("2023-08-16T11:30:00Z");
            Duration maxRuntime = Duration.ofHours(2);
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"),
                    maxRuntime);

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenStartedAndMetMaxRuntimeDuringWindow() {
            Instant startTime = Instant.parse("2023-08-16T12:15:00Z");
            Duration maxRuntime = Duration.ofMinutes(30);
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"),
                    maxRuntime);

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenStartedDuringWindowAndMaxRuntimeIsAfterWindow() {
            Instant startTime = Instant.parse("2023-08-16T12:15:00Z");
            Duration maxRuntime = Duration.ofHours(1);
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"),
                    maxRuntime);

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isTrue();
        }

        @Test
        void shouldNotBeInPeriodWhenStartedAfterWindowWithMaxRuntime() {
            Instant startTime = Instant.parse("2023-08-16T13:15:00Z");
            Duration maxRuntime = Duration.ofHours(1);
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z"),
                    maxRuntime);

            assertThat(timeWindowQuery.isUnfinishedJobInWindow(startTime))
                    .isFalse();
        }
    }

}
