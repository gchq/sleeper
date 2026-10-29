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
package sleeper.compaction.core.job;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Region;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * A bundle of the inputs required to execute a compaction.
 * <p>
 * Contains the job, the configuration of the Sleeper table the job runs in, the region to read, and a progress
 * callback. The progress callback receives updates on the number of rows read by a compaction.
 *
 * <p>The progress callback is always non-null. If not supplied to the builder, a no-op consumer is used.
 */
public class CompactionRequest {

    /** Default interval between progress callback invocations, matching the Rust default. */
    public static final Duration DEFAULT_PROGRESS_CALLBACK_INTERVAL = Duration.ofSeconds(5);

    private static final LongConsumer NO_OP_PROGRESS_CALLBACK = rows -> {
    };

    private final CompactionJob job;
    private final TableProperties tableProperties;
    private final Region region;
    private final LongConsumer progressCallback;
    private final Duration progressCallbackInterval;

    private CompactionRequest(Builder builder) {
        this.job = Objects.requireNonNull(builder.job, "job must not be null");
        this.tableProperties = Objects.requireNonNull(builder.tableProperties, "tableProperties must not be null");
        this.region = Objects.requireNonNull(builder.region, "region must not be null");
        this.progressCallback = builder.progressCallback == null ? NO_OP_PROGRESS_CALLBACK : builder.progressCallback;
        this.progressCallbackInterval = builder.progressCallbackInterval == null
                ? DEFAULT_PROGRESS_CALLBACK_INTERVAL : builder.progressCallbackInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    public CompactionJob getJob() {
        return job;
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }

    public Region getRegion() {
        return region;
    }

    public LongConsumer getProgressCallback() {
        return progressCallback;
    }

    public Duration getProgressCallbackInterval() {
        return progressCallbackInterval;
    }

    public static class Builder {
        private CompactionJob job;
        private TableProperties tableProperties;
        private Region region;
        private LongConsumer progressCallback;
        private Duration progressCallbackInterval;

        private Builder() {
        }

        public Builder job(CompactionJob job) {
            this.job = job;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder region(Region region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the progress callback to receive notifications of compaction progress.
         *
         * The consumer will receive progress notifications of the number of rows read (not necessarily written)
         * by the compaction. The written row count may be different due to filtering or aggregation.
         *
         * If not set or set to null, the built request will use a no-op consumer.
         *
         * @param  progressCallback the callback, or null to use a no-op
         * @return                  this builder
         */
        public Builder progressCallback(LongConsumer progressCallback) {
            this.progressCallback = progressCallback;
            return this;
        }

        /**
         * Sets the interval between progress callback invocations by the native compaction runner.
         * <p>
         * If not set or set to null, the built request will use the default interval of 5 seconds.
         *
         * @param  progressCallbackInterval the interval, or null to use the default
         * @return                          this builder
         */
        public Builder progressCallbackInterval(Duration progressCallbackInterval) {
            this.progressCallbackInterval = progressCallbackInterval;
            return this;
        }

        public CompactionRequest build() {
            return new CompactionRequest(this);
        }
    }
}
