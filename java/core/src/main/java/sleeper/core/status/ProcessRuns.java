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
    private final List<ProcessRun> latestFirst;

    private ProcessRuns(List<ProcessRun> latestFirst) {
        this.latestFirst = Collections.unmodifiableList(Objects.requireNonNull(latestFirst, "latestFirst must not be null"));
    }

    public static ProcessRuns latestFirst(List<ProcessRun> latestFirst) {
        return new ProcessRuns(latestFirst);
    }

    public boolean isStarted() {
        return !latestFirst.isEmpty();
    }

    public boolean isFinished() {
        return !latestFirst.isEmpty() && latestFirst.stream().allMatch(ProcessRun::isFinished);
    }

    public boolean isTaskIdAssigned(String taskId) {
        return latestFirst.stream().anyMatch(run -> run.getTaskId().equals(taskId));
    }

    public Optional<Instant> lastTime() {
        if (getLatestRun().isPresent()) {
            return Optional.of(getLatestRun().get().getLatestUpdateTime());
        }
        return Optional.empty();
    }

    private Optional<ProcessRun> getLatestRun() {
        if (!latestFirst.isEmpty()) {
            return Optional.of(latestFirst.get(0));
        }
        return Optional.empty();
    }

    public List<ProcessRun> getRunList() {
        return latestFirst;
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
        return latestFirst.equals(that.latestFirst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestFirst);
    }

}
