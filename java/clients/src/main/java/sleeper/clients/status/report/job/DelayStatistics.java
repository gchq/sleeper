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

public class DelayStatistics {
    private final Duration minDelay;
    private final Duration avgDelay;
    private final Duration maxDelay;

    public DelayStatistics(Builder builder) {
        this.minDelay = Duration.ofMillis(builder.minDelayMillis);
        this.avgDelay = Duration.ofMillis(builder.avgDelayMillis);
        this.maxDelay = Duration.ofMillis(builder.maxDelayMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String toString() {
        return "Average delay: " + LoggedDuration.withShortOutput(avgDelay) + "\n"
                + "Shortest delay: " + LoggedDuration.withShortOutput(minDelay) + "\n"
                + "Longest delay: " + LoggedDuration.withShortOutput(maxDelay);
    }

    public static class Builder {
        private List<Long> delays;
        private long minDelayMillis;
        private long avgDelayMillis;
        private long maxDelayMillis;
        private long totalDelay;
        private long delayCount;
        private double standardDeviation;

        public Builder add(Duration delay) {
            if (delayCount == 0) {
                minDelayMillis = delay.toMillis();
            } else {
                minDelayMillis = Math.min(delay.toMillis(), minDelayMillis);
            }
            maxDelayMillis = Math.max(delay.toMillis(), maxDelayMillis);
            totalDelay += delay.toMillis();
            delayCount++;
            return this;
        }

        public DelayStatistics build() {
            avgDelayMillis = totalDelay / delayCount;
            return new DelayStatistics(this);
        }
    }
}
