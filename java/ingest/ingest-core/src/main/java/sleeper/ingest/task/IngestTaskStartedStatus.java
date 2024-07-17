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
package sleeper.ingest.task;

import sleeper.core.record.process.status.ProcessRunStartedUpdate;

import java.time.Instant;
import java.util.Objects;

/**
 * A status for ingest tasks that have started.
 */
public class IngestTaskStartedStatus implements ProcessRunStartedUpdate {

    private final Instant startTime;

    private IngestTaskStartedStatus(Instant startTime) {
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
    }

    /**
     * Creates an instance of this class with the start time set.
     *
     * @param  startTime the start time
     * @return           an instance of this class
     */
    public static IngestTaskStartedStatus startTime(Instant startTime) {
        return new IngestTaskStartedStatus(startTime);
    }

    @Override
    public Instant getUpdateTime() {
        return startTime;
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestTaskStartedStatus that = (IngestTaskStartedStatus) o;
        return startTime.equals(that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime);
    }

    @Override
    public String toString() {
        return "IngestTaskStartedStatus{" +
                "startTime=" + startTime +
                '}';
    }
}
