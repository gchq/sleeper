/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.job.status;

import sleeper.core.record.process.status.ProcessRunStartedUpdate;
import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

public class IngestJobStartedStatus implements ProcessRunStartedUpdate {

    private final int inputFileCount;
    private final Instant startTime;
    private final Instant updateTime;

    private IngestJobStartedStatus(int inputFileCount, Instant updateTime, Instant startTime) {
        this.inputFileCount = inputFileCount;
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
    }

    public static IngestJobStartedStatus updateAndStartTime(IngestJob job, Instant updateTime, Instant startTime) {
        return new IngestJobStartedStatus(job.getFiles().size(), updateTime, startTime);
    }

    public int getInputFileCount() {
        return inputFileCount;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
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
        IngestJobStartedStatus that = (IngestJobStartedStatus) o;
        return updateTime.equals(that.updateTime) && startTime.equals(that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, startTime);
    }

    @Override
    public String toString() {
        return "IngestJobStartedStatus{" +
                "updateTime=" + updateTime +
                ", startTime=" + startTime +
                '}';
    }
}
