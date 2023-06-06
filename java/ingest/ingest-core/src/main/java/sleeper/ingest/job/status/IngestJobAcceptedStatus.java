/*
 * Copyright 2022-2023 Crown Copyright
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

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

public class IngestJobAcceptedStatus implements IngestJobValidatedStatus {
    private final Instant validationTime;
    private final Instant updateTime;
    private final int inputFileCount;

    public IngestJobAcceptedStatus(int inputFileCount, Instant updateTime) {
        this(inputFileCount, updateTime, updateTime);
    }

    public IngestJobAcceptedStatus(int inputFileCount, Instant validationTime, Instant updateTime) {
        this.validationTime = validationTime;
        this.updateTime = updateTime;
        this.inputFileCount = inputFileCount;
    }

    public static IngestJobAcceptedStatus from(IngestJob job, Instant validationTime, Instant updateTime) {
        return new IngestJobAcceptedStatus(job.getFiles().size(), validationTime, updateTime);
    }

    public static IngestJobAcceptedStatus validationTime(Instant updateTime) {
        return new IngestJobAcceptedStatus(0, updateTime);
    }

    @Override
    public Instant getStartTime() {
        return validationTime;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    public int getInputFileCount() {
        return inputFileCount;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobAcceptedStatus that = (IngestJobAcceptedStatus) o;
        return inputFileCount == that.inputFileCount && Objects.equals(updateTime, that.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, inputFileCount);
    }

    @Override
    public String toString() {
        return "IngestJobAcceptedStatus{" +
                "updateTime=" + updateTime +
                ", inputFileCount=" + inputFileCount +
                '}';
    }
}
