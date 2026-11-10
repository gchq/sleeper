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

import sleeper.core.properties.model.CompactionProgressCheckSettings;
import sleeper.core.util.LoggedDuration;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Decides whether a running compaction has stalled, based on the progress updates it has sent.
 */
public class CompactionStallDetector {

    private final Duration firstUpdateTimeout;
    private final Duration stallTimeout;

    public CompactionStallDetector(Duration firstUpdateTimeout, Duration stallTimeout) {
        this.firstUpdateTimeout = firstUpdateTimeout;
        this.stallTimeout = stallTimeout;
    }

    /**
     * Creates a stall detector with timeouts from the given settings.
     *
     * @param  settings the progress check settings
     * @return          the stall detector
     */
    public static CompactionStallDetector from(CompactionProgressCheckSettings settings) {
        return new CompactionStallDetector(settings.firstUpdateTimeout(), settings.stallTimeout());
    }

    /**
     * Checks whether a compaction has stalled.
     *
     * @param  jobStartTime the time the compaction started
     * @param  progress     the latest progress, or an empty optional if no update has been received
     * @param  now          the current time
     * @return              details of the stall, or an empty optional if the compaction has not stalled
     */
    public Optional<CompactionStall> checkForStall(Instant jobStartTime, Optional<CompactionProgress> progress, Instant now) {
        if (progress.isEmpty()) {
            Duration sinceStart = Duration.between(jobStartTime, now);
            if (sinceStart.compareTo(firstUpdateTimeout) >= 0) {
                return Optional.of(new CompactionStall(String.format(
                        "No progress update received in %s since compaction started, exceeding maximum of %s",
                        LoggedDuration.withShortOutput(sinceStart),
                        LoggedDuration.withShortOutput(firstUpdateTimeout)), now));
            }
            return Optional.empty();
        }
        CompactionProgress latest = progress.get();
        Duration sinceChange = Duration.between(latest.lastChangeTime(), now);
        if (sinceChange.compareTo(stallTimeout) >= 0) {
            return Optional.of(new CompactionStall(String.format(
                    "Rows processed unchanged at %s for %s, exceeding maximum of %s. Last progress update received %s ago",
                    latest.rowsProcessed(),
                    LoggedDuration.withShortOutput(sinceChange),
                    LoggedDuration.withShortOutput(stallTimeout),
                    LoggedDuration.withShortOutput(latest.lastUpdateTime(), now)), now));
        }
        return Optional.empty();
    }
}
