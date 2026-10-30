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
package sleeper.compaction.core.task;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionStallDetectorTest {

    private static final Instant JOB_START = Instant.parse("2026-10-26T10:00:00Z");

    private final CompactionStallDetector detector = new CompactionStallDetector(
            Duration.ofMinutes(10), Duration.ofMinutes(5));

    @Nested
    @DisplayName("No progress update received yet")
    class NoUpdateYet {

        @Test
        void shouldNotStallBeforeFirstUpdateTimeout() {
            assertThat(detector.checkForStall(JOB_START, Optional.empty(),
                    Instant.parse("2026-10-26T10:09:59Z")))
                    .isEmpty();
        }

        @Test
        void shouldStallAtFirstUpdateTimeout() {
            Instant now = Instant.parse("2026-10-26T10:10:00Z");
            assertThat(detector.checkForStall(JOB_START, Optional.empty(), now))
                    .contains(new CompactionStall(
                            "No progress update received in 10m 0s since compaction started, exceeding maximum of 10m 0s",
                            now));
        }
    }

    @Nested
    @DisplayName("Progress updates received")
    class UpdatesReceived {

        @Test
        void shouldNotStallWhenRowsChangedRecently() {
            CompactionProgress progress = new CompactionProgress(100,
                    Instant.parse("2026-10-26T10:20:00Z"),
                    Instant.parse("2026-10-26T10:20:00Z"));
            assertThat(detector.checkForStall(JOB_START, Optional.of(progress),
                    Instant.parse("2026-10-26T10:24:59Z")))
                    .isEmpty();
        }

        @Test
        void shouldIgnoreFirstUpdateTimeoutOnceUpdateReceived() {
            CompactionProgress progress = new CompactionProgress(100,
                    Instant.parse("2026-10-26T10:15:00Z"),
                    Instant.parse("2026-10-26T10:15:00Z"));
            assertThat(detector.checkForStall(JOB_START, Optional.of(progress),
                    Instant.parse("2026-10-26T10:16:00Z")))
                    .isEmpty();
        }

        @Test
        void shouldStallWhenRowsUnchangedWhileUpdatesContinue() {
            CompactionProgress progress = new CompactionProgress(100,
                    Instant.parse("2026-10-26T10:24:55Z"),
                    Instant.parse("2026-10-26T10:20:00Z"));
            Instant now = Instant.parse("2026-10-26T10:25:00Z");
            assertThat(detector.checkForStall(JOB_START, Optional.of(progress), now))
                    .contains(new CompactionStall(
                            "Rows processed unchanged at 100 for 5m 0s, exceeding maximum of 5m 0s. " +
                                    "Last progress update received 5s ago",
                            now));
        }

        @Test
        void shouldStallWhenUpdatesStop() {
            CompactionProgress progress = new CompactionProgress(100,
                    Instant.parse("2026-10-26T10:20:00Z"),
                    Instant.parse("2026-10-26T10:20:00Z"));
            Instant now = Instant.parse("2026-10-26T10:25:00Z");
            assertThat(detector.checkForStall(JOB_START, Optional.of(progress), now))
                    .contains(new CompactionStall(
                            "Rows processed unchanged at 100 for 5m 0s, exceeding maximum of 5m 0s. " +
                                    "Last progress update received 5m 0s ago",
                            now));
        }
    }
}
