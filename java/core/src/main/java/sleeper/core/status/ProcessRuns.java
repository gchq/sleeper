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

package sleeper.core.status;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ProcessRuns {
    private final List<ProcessRun> jobRunList;

    private ProcessRuns(Builder builder) {
        jobRunList = builder.jobRunList;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isStarted() {
        return !jobRunList.isEmpty();
    }

    public boolean isFinished() {
        return !jobRunList.isEmpty() && jobRunList.stream().allMatch(ProcessRun::isFinished);
    }

    public boolean isTaskIdAssigned(String taskId) {
        return jobRunList.stream().anyMatch(run -> run.getTaskId().equals(taskId));
    }

    public Optional<Instant> lastTime() {
        if (getLatestJobRun().isPresent()) {
            return Optional.of(getLatestJobRun().get().getLatestUpdateTime());
        }
        return Optional.empty();
    }

    private Optional<ProcessRun> getLatestJobRun() {
        return Optional.ofNullable(jobRunList.get(0));
    }

    public List<ProcessRun> getJobRunList() {
        return jobRunList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessRuns that = (ProcessRuns) o;
        return jobRunList.equals(that.jobRunList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobRunList);
    }

    public static final class Builder {
        private List<ProcessRun> jobRunList;

        private Builder() {
        }

        public Builder jobRunList(List<ProcessRun> jobRunList) {
            this.jobRunList = jobRunList;
            return this;
        }

        public Builder singleJobRun(ProcessRun jobRun) {
            this.jobRunList = Collections.singletonList(jobRun);
            return this;
        }

        public Builder jobRunsLatestFirst(List<ProcessRun> jobRunList) {
            this.jobRunList = jobRunList;
            return this;
        }

        public ProcessRuns build() {
            return new ProcessRuns(this);
        }
    }
}
