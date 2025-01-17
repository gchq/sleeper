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

import sleeper.core.tracker.job.run.JobRunSummary;
import sleeper.core.tracker.job.run.RecordsProcessed;

import java.time.Instant;
import java.util.Objects;

/**
 * An implementation of a job status update that is both the start and finish of a job run. Only used in tests.
 */
public class TestJobStartedAndFinishedStatus implements JobRunStartedUpdate, JobRunEndUpdate {

    private final Instant updateTime;
    private final JobRunSummary summary;

    private TestJobStartedAndFinishedStatus(Instant updateTime, JobRunSummary summary) {
        this.updateTime = Objects.requireNonNull(updateTime, "updateTime may not be null");
        this.summary = Objects.requireNonNull(summary, "summary may not be null");
    }

    /**
     * Creates an instance of this class.
     *
     * @param  updateTime the update time
     * @param  summary    the job run summary
     * @return            an instance of this class
     */
    public static TestJobStartedAndFinishedStatus updateAndSummary(Instant updateTime, JobRunSummary summary) {
        return new TestJobStartedAndFinishedStatus(updateTime, summary);
    }

    @Override
    public Instant getUpdateTime() {
        return updateTime;
    }

    @Override
    public Instant getStartTime() {
        return summary.getStartTime();
    }

    @Override
    public Instant getFinishTime() {
        return summary.getFinishTime();
    }

    @Override
    public RecordsProcessed getRecordsProcessed() {
        return summary.getRecordsProcessed();
    }

    @Override
    public boolean isPartOfRun() {
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
        TestJobStartedAndFinishedStatus that = (TestJobStartedAndFinishedStatus) o;
        return updateTime.equals(that.updateTime) && summary.equals(that.summary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updateTime, summary);
    }

    @Override
    public String toString() {
        return "TestJobStartedAndFinishedStatus{" +
                "updateTime=" + updateTime +
                ", summary=" + summary +
                '}';
    }
}
