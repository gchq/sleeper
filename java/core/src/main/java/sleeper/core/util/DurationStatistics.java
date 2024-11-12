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
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Calculates statistics about a dataset of Duration objects.
 */
public class DurationStatistics {
    private final Duration min;
    private final Duration mean;
    private final Duration percent99;
    private final Duration percent999;
    private final Duration max;
    private final Duration standardDeviation;

    public DurationStatistics(Builder builder) {
        this.min = builder.min;
        this.mean = builder.mean;
        this.percent99 = builder.percent99;
        this.percent999 = builder.percent999;
        this.max = builder.max;
        this.standardDeviation = builder.standardDeviation;
    }

    /**
     * Calculates statistics from a stream of Duration objects, if any are present.
     *
     * @param  durations the durations
     * @return           the statistics, if any are present
     */
    public static Optional<DurationStatistics> fromIfAny(Stream<Duration> durations) {
        List<Long> durationsInMills = durations.map(Duration::toMillis).collect(toUnmodifiableList());
        if (durationsInMills.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(builder()
                    .computeFromMilliseconds(durationsInMills)
                    .build());
        }
    }

    private static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return String.format("avg: %s, min: %s, 99%%: %s, 99.9%%: %s, max: %s, std dev: %s",
                LoggedDuration.withShortOutput(mean),
                LoggedDuration.withShortOutput(min),
                LoggedDuration.withShortOutput(percent99),
                LoggedDuration.withShortOutput(percent999),
                LoggedDuration.withShortOutput(max),
                LoggedDuration.withShortOutput(standardDeviation));
    }

    /**
     * Builds instances of this class.
     */
    private static class Builder {
        private Duration min;
        private Duration mean;
        private Duration percent99;
        private Duration percent999;
        private Duration max;
        private Duration standardDeviation;

        Builder computeFromMilliseconds(List<Long> durationsInMilliseconds) {
            List<Long> sortedMilliseconds = durationsInMilliseconds.stream()
                    .sorted().collect(toUnmodifiableList());
            int n = sortedMilliseconds.size();
            double meanMillis = sortedMilliseconds.stream()
                    .mapToLong(millis -> millis).sum() / (double) n;
            double variance = sortedMilliseconds.stream()
                    .mapToDouble(millis -> Math.pow(millis - meanMillis, 2))
                    .sum() / n;
            min = Duration.ofMillis(sortedMilliseconds.get(0));
            mean = Duration.ofMillis((long) meanMillis);
            percent99 = Duration.ofMillis(getPercentile(sortedMilliseconds, 99.0));
            percent999 = Duration.ofMillis(getPercentile(sortedMilliseconds, 99.9));
            max = Duration.ofMillis(sortedMilliseconds.get(sortedMilliseconds.size() - 1));
            standardDeviation = Duration.ofMillis((long) Math.sqrt(variance));
            return this;
        }

        DurationStatistics build() {
            return new DurationStatistics(this);
        }

        private static <T> T getPercentile(List<T> sorted, double percentile) {
            int rank = percentile == 0 ? 1 : (int) Math.ceil(percentile / 100.0 * sorted.size());
            return sorted.get(rank - 1);
        }
    }
}
