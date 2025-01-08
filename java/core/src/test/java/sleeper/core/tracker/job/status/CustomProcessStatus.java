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

package sleeper.core.tracker.job.status;

import java.time.Instant;
import java.util.Objects;

/**
 * A test implementation of process status update.
 */
public class CustomProcessStatus implements ProcessStatusUpdate {
    private final Instant updateTime;
    private final boolean isPartOfRun;

    private CustomProcessStatus(Instant updateTime, boolean isPartOfRun) {
        this.updateTime = updateTime;
        this.isPartOfRun = isPartOfRun;
    }

    /**
     * Creates an instance of this class that is marked as part of a job run.
     *
     * @param  updateTime the update time
     * @return            an instance of this class that is marked as part of a job run
     */
    public static CustomProcessStatus partOfRunWithUpdateTime(Instant updateTime) {
        return new CustomProcessStatus(updateTime, true);
    }

    /**
     * Creates an instance of this class that is not marked as part of a job run.
     *
     * @param  updateTime the update time
     * @return            an instance of this class that is not marked as part of a job run
     */
    public static CustomProcessStatus notPartOfRunWithUpdateTime(Instant updateTime) {
        return new CustomProcessStatus(updateTime, false);
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public boolean isPartOfRun() {
        return isPartOfRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomProcessStatus that = (CustomProcessStatus) o;
        return Objects.equals(updateTime, that.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime);
    }

    @Override
    public String toString() {
        return "CustomProcessStatus{" +
                "updateTime=" + updateTime +
                '}';
    }
}
