/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.core.record.process.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeWindowQueryTest {
    @Nested
    @DisplayName("Process started")
    class ProcessStarted {
        @Test
        void shouldBeInPeriodWhereStartTimeIsBeforePeriodStartTime() {
            Instant startTime = Instant.parse("2023-08-16T11:00:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z")
            );

            assertThat(timeWindowQuery.isInWindow(startTime, startTime))
                    .isTrue();
        }

        @Test
        void shouldBeInPeriodWhenStartTimeIsAfterPeriodStartTimeButBeforePeriodEndTime() {
            Instant startTime = Instant.parse("2023-08-16T12:30:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z")
            );

            assertThat(timeWindowQuery.isInWindow(startTime, startTime))
                    .isTrue();
        }

        @Test
        void shouldNotBeInPeriodWhenStartTimeIsAfterPeriodEndTime() {
            Instant startTime = Instant.parse("2023-08-16T14:00:00Z");
            TimeWindowQuery timeWindowQuery = new TimeWindowQuery(
                    Instant.parse("2023-08-16T12:00:00Z"),
                    Instant.parse("2023-08-16T13:00:00Z")
            );

            assertThat(timeWindowQuery.isInWindow(startTime, startTime))
                    .isFalse();
        }
    }
}
