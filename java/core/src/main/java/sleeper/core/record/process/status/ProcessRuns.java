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

package sleeper.core.record.process.status;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class ProcessRuns {
    private final List<ProcessRun> latestFirst;

    private ProcessRuns(List<ProcessRun> latestFirst) {
        this.latestFirst = Collections.unmodifiableList(Objects.requireNonNull(latestFirst, "latestFirst must not be null"));
    }

    public static ProcessRuns latestFirst(List<ProcessRun> latestFirst) {
        return new ProcessRuns(latestFirst);
    }

    public static ProcessRuns fromRecordsLatestFirst(List<ProcessStatusUpdateRecord> recordList) {
        Map<String, ProcessRun.Builder> taskBuilders = new HashMap<>();
        List<ProcessRun.Builder> orderedBuilders = new ArrayList<>();
        for (int i = recordList.size() - 1; i >= 0; i--) {
            ProcessStatusUpdateRecord record = recordList.get(i);
            String taskId = record.getTaskId();
            ProcessStatusUpdate statusUpdate = record.getStatusUpdate();
            if (statusUpdate.isStartOfRun()) {
                ProcessRun.Builder builder = ProcessRun.builder()
                        .statusUpdate(statusUpdate)
                        .taskId(taskId);
                taskBuilders.put(taskId, builder);
                orderedBuilders.add(builder);
            } else if ((statusUpdate instanceof ProcessFinishedStatus)
                    && taskBuilders.containsKey(taskId)) {
                taskBuilders.remove(taskId)
                        .finishedStatus((ProcessFinishedStatus) statusUpdate)
                        .taskId(taskId);
            }
        }
        List<ProcessRun> jobRuns = orderedBuilders.stream()
                .map(ProcessRun.Builder::build)
                .collect(Collectors.toList());
        Collections.reverse(jobRuns);
        return new ProcessRuns(jobRuns);
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
        return getLatestRun().map(ProcessRun::getLatestUpdateTime);
    }

    public Optional<Instant> firstTime() {
        return getFirstRun().map(ProcessRun::getStartUpdateTime);
    }

    public Optional<ProcessRun> getLatestRun() {
        return latestFirst.stream().findFirst();
    }

    public Optional<ProcessRun> getFirstRun() {
        if (latestFirst.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(latestFirst.get(latestFirst.size() - 1));
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

    @Override
    public String toString() {
        return "ProcessRuns{" +
                "latestFirst=" + latestFirst +
                '}';
    }
}
