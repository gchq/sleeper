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
package sleeper.core.util;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Calculates statistics about a dataset of Duration objects.
 */
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

    /**
     * Calculates statistics from a stream of Duration objects.
     *
     * @param  durations the durations
     * @return           the statistics
     */
    public static DurationStatistics from(Stream<Duration> durations) {
        return builder()
                .computeFromMilliseconds(durations
                        .map(Duration::toMillis)
                        .collect(toUnmodifiableList()))
                .build();
    }

    private static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("avg: %s, min: %s, max: %s, std dev: %s",
                LoggedDuration.withShortOutput(mean),
                LoggedDuration.withShortOutput(min),
                LoggedDuration.withShortOutput(max),
                LoggedDuration.withShortOutput(standardDeviation));
    }

    /**
     * Builds instances of this class.
     */
    private static class Builder {
        private long minMillis = Long.MAX_VALUE;
        private long meanMillis;
        private long maxMillis = Long.MIN_VALUE;
        private long stdDevMillis = 0;

        Builder computeFromMilliseconds(List<Long> durationsInMilliseconds) {
            int n = durationsInMilliseconds.size();
            double mean = durationsInMilliseconds.stream()
                    .peek(millis -> minMillis = Math.min(millis, minMillis))
                    .peek(millis -> maxMillis = Math.max(millis, maxMillis))
                    .mapToLong(millis -> millis).sum() / (double) n;
            double variance = durationsInMilliseconds.stream()
                    .mapToDouble(millis -> Math.pow(millis - mean, 2))
                    .sum() / n;
            meanMillis = (long) mean;
            stdDevMillis = (long) Math.sqrt(variance);
            return this;
        }

        DurationStatistics build() {
            return new DurationStatistics(this);
        }
    }
}
