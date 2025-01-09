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

import static sleeper.core.tracker.job.status.JobStatusUpdateTestHelper.defaultUpdateTime;

/**
 * A test implementation of a job run started update. This class allows for setting the
 * {@link JobRunStartedUpdate#isStartOfRun} flag.
 */
public class TestJobStartedStatusWithStartOfRunFlag implements JobRunStartedUpdate {

    private final Instant updateTime;
    private final Instant startTime;
    private final boolean isStartOfRun;

    private TestJobStartedStatusWithStartOfRunFlag(Instant updateTime, Instant startTime, boolean isStartOfRun) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.startTime = Objects.requireNonNull(startTime, "startTime may not be null");
        this.isStartOfRun = isStartOfRun;
    }

    /**
     * Creates an instance of this class that is not marked as being the start of a job run.
     *
     * @param  startTime the start time
     * @return           an instance of this class that is not marked as being the start of a job run
     */
    public static TestJobStartedStatusWithStartOfRunFlag startedStatusNotStartOfRun(Instant startTime) {
        return new TestJobStartedStatusWithStartOfRunFlag(defaultUpdateTime(startTime), startTime, false);
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public boolean isStartOfRun() {
        return isStartOfRun;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestJobStartedStatusWithStartOfRunFlag that = (TestJobStartedStatusWithStartOfRunFlag) o;
        return isStartOfRun == that.isStartOfRun && Objects.equals(updateTime, that.updateTime) && Objects.equals(startTime, that.startTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, startTime, isStartOfRun);
    }

    @Override
    public String toString() {
        return "TestJobStartedStatusWithStartOfRunFlag{" +
                "updateTime=" + updateTime +
                ", startTime=" + startTime +
                ", isStartOfRun=" + isStartOfRun +
                '}';
    }
}
