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
import java.util.ArrayList;
import java.util.List;

public class DelayStatistics {
    private final Duration minDelay;
    private final Duration avgDelay;
    private final Duration maxDelay;
    private final Duration standardDeviation;

    public DelayStatistics(Builder builder) {
        this.minDelay = Duration.ofMillis(builder.minDelayMillis);
        this.avgDelay = Duration.ofMillis(builder.avgDelayMillis);
        this.maxDelay = Duration.ofMillis(builder.maxDelayMillis);
        this.standardDeviation = Duration.ofMillis(builder.stdDevMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String toString() {
        return String.format("avg: %s, min: %s, max: %s, std dev: %s",
                LoggedDuration.withShortOutput(avgDelay),
                LoggedDuration.withShortOutput(minDelay),
                LoggedDuration.withShortOutput(maxDelay),
                LoggedDuration.withShortOutput(standardDeviation));
    }

    public static class Builder {
        private List<Long> delays = new ArrayList<>();
        private long minDelayMillis;
        private long avgDelayMillis;
        private long maxDelayMillis;
        private long totalDelay;
        private long delayCount;
        private long stdDevMillis = 0;

        public Builder add(Duration delay) {
            delays.add(delay.toMillis());
            return this;
        }

        public DelayStatistics build() {
            delays.forEach(delay -> {
                if (delayCount == 0) {
                    minDelayMillis = delay;
                } else {
                    minDelayMillis = Math.min(delay, minDelayMillis);
                }
                maxDelayMillis = Math.max(delay, maxDelayMillis);
                totalDelay += delay;
                delayCount++;
            });
            avgDelayMillis = totalDelay / delayCount;
            double sumOfAvgDiffSquares = 0;
            for (long delay : delays) {
                sumOfAvgDiffSquares += Math.pow(delay - avgDelayMillis, 2);
            }
            stdDevMillis = (long) Math.sqrt(sumOfAvgDiffSquares / delayCount);
            return new DelayStatistics(this);
        }
    }
}
