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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class ProcessRunsBuilder {
    private final Map<String, ProcessRun.Builder> builderByRunId = new HashMap<>();
    private final Map<String, ProcessRun.Builder> builderByTaskId = new HashMap<>();
    private final List<ProcessRun.Builder> orderedBuilders = new ArrayList<>();

    void add(ProcessStatusUpdateRecord record) {
        String runId = record.getRunId();
        String taskId = record.getTaskId();
        ProcessStatusUpdate statusUpdate = record.getStatusUpdate();
        if (isStartedUpdateAndStartOfRun(statusUpdate)) {
            ProcessRun.Builder builder = ProcessRun.builder()
                    .startedStatus((ProcessRunStartedUpdate) statusUpdate)
                    .taskId(taskId);
            if (runId != null) {
                builderByRunId.put(runId, builder);
            } else {
                builderByTaskId.put(taskId, builder);
            }
            orderedBuilders.add(builder);
        } else if (builderByRunId.containsKey(runId)) {
            addToBuilderByKey(statusUpdate, builderByRunId, runId)
                    .ifPresent(builder -> {
                        if (taskId != null) {
                            builder.taskId(taskId);
                        }
                    });
        } else if (builderByTaskId.containsKey(taskId)) {
            addToBuilderByKey(statusUpdate, builderByTaskId, taskId);
        }
    }

    private Optional<ProcessRun.Builder> addToBuilderByKey(
            ProcessStatusUpdate statusUpdate, Map<String, ProcessRun.Builder> builderMap, String key) {
        if (statusUpdate instanceof ProcessFinishedStatus) {
            return Optional.of(builderMap.remove(key)
                    .finishedStatus((ProcessFinishedStatus) statusUpdate));
        } else if (statusUpdate.isPartOfRun()) {
            return Optional.of(builderMap.get(key)
                    .statusUpdate(statusUpdate));
        }
        return Optional.empty();
    }

    ProcessRuns build() {
        List<ProcessRun> jobRuns = orderedBuilders.stream()
                .map(ProcessRun.Builder::build)
                .collect(Collectors.toList());
        Collections.reverse(jobRuns);
        return new ProcessRuns(jobRuns);
    }

    private static boolean isStartedUpdateAndStartOfRun(ProcessStatusUpdate statusUpdate) {
        return statusUpdate instanceof ProcessRunStartedUpdate
                && ((ProcessRunStartedUpdate) statusUpdate).isStartOfRun();
    }
}
