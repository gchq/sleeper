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
package sleeper.clients.status.report.job;

import sleeper.core.util.LoggedDuration;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

public class DurationStatistics {
    private final Duration min;
    private final Duration mean;
    private final Duration max;
    private final Duration standardDeviation;

    public DurationStatistics(Builder builder) {
        this.min = Duration.ofMillis(builder.minMillis);
        this.mean = Duration.ofMillis(builder.meanMillis);
        this.max = Duration.ofMillis(builder.maxMillis);
        this.standardDeviation = Duration.ofMillis(builder.stdDevMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static DurationStatistics from(Stream<Duration> durations) {
        return builder()
                .computeFromMilliseconds(durations
                        .map(Duration::toMillis)
                        .collect(toUnmodifiableList()))
                .build();
    }

    public String toString() {
        return String.format("avg: %s, min: %s, max: %s, std dev: %s",
                LoggedDuration.withShortOutput(mean),
                LoggedDuration.withShortOutput(min),
                LoggedDuration.withShortOutput(max),
                LoggedDuration.withShortOutput(standardDeviation));
    }

    public static class Builder {
        private long minMillis;
        private long meanMillis;
        private long maxMillis;
        private long totalMillis;
        private long delayCount;
        private long stdDevMillis = 0;

        public Builder computeFromMilliseconds(List<Long> durationsInMilliseconds) {
            durationsInMilliseconds.forEach(millis -> {
                if (delayCount == 0) {
                    minMillis = millis;
                } else {
                    minMillis = Math.min(millis, minMillis);
                }
                maxMillis = Math.max(millis, maxMillis);
                totalMillis += millis;
                delayCount++;
            });
            meanMillis = totalMillis / delayCount;
            double sumOfAvgDiffSquares = 0;
            for (long millis : durationsInMilliseconds) {
                sumOfAvgDiffSquares += Math.pow(millis - meanMillis, 2);
            }
            stdDevMillis = (long) Math.sqrt(sumOfAvgDiffSquares / delayCount);
            return this;
        }

        public DurationStatistics build() {
            return new DurationStatistics(this);
        }
    }
}
