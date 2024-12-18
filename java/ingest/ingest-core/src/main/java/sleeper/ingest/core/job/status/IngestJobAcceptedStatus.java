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

package sleeper.ingest.core.job.status;

import java.time.Instant;
import java.util.Objects;

/**
 * An ingest job validated status for when the job was validated successfully.
 */
public class IngestJobAcceptedStatus implements IngestJobValidatedStatus {
    private final Instant validationTime;
    private final Instant updateTime;
    private final int inputFileCount;

    private IngestJobAcceptedStatus(int inputFileCount, Instant validationTime, Instant updateTime) {
        this.validationTime = Objects.requireNonNull(validationTime, "validationTime must not be null");
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime must not be null");
        this.inputFileCount = inputFileCount;
    }

    /**
     * Creates an instance of this class.
     *
     * @param  inputFileCount the input file count
     * @param  validationTime the validation time
     * @param  updateTime     the update time
     * @return                an instance of this class
     */
    public static IngestJobAcceptedStatus from(int inputFileCount, Instant validationTime, Instant updateTime) {
        return new IngestJobAcceptedStatus(inputFileCount, validationTime, updateTime);
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
        return inputFileCount == that.inputFileCount
                && Objects.equals(validationTime, that.validationTime)
                && Objects.equals(updateTime, that.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(validationTime, updateTime, inputFileCount);
    }

    @Override
    public String toString() {
        return "IngestJobAcceptedStatus{" +
                "validationTime=" + validationTime +
                ", updateTime=" + updateTime +
                ", inputFileCount=" + inputFileCount +
                '}';
    }
}
