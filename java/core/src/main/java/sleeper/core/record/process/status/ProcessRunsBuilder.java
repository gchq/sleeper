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
package sleeper.core.record.process.status;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Gathers status store records and correlates which updates occurred in the same run of a job. Creates a
 * {@link ProcessRuns} object with the detected runs.
 * <p>
 * Records are processed in order of update time. A run is detected based on the type of the update. Some updates mark
 * the start of a run. Once a run has started, further updates on the same task are assumed to be part of that run,
 * until another update starts a new run on the same task. Updates on a task before any run has started are ignored.
 * Updates that happen on different tasks are always in different runs.
 * <p>
 * Some jobs may include a run ID, which explicitly correlates updates on the same job run. In this case it is not
 * necessary to assume updates on the same task are part of the same run. Updates before the run has started are still
 * ignored.
 * <p>
 * Some updates do not occur on any run, particularly ones which do not occur on a task. Those updates will not appear
 * in the resulting {@link ProcessRuns} object, and must be handled separately.
 */
class ProcessRunsBuilder {
    private final Map<String, ProcessRun.Builder> builderByJobRunId = new HashMap<>();
    private final Map<String, ProcessRun.Builder> builderByTaskId = new HashMap<>();
    private final List<ProcessRun.Builder> orderedBuilders = new ArrayList<>();

    void add(ProcessStatusUpdateRecord record) {
        String jobRunId = record.getJobRunId();
        String taskId = record.getTaskId();
        ProcessStatusUpdate statusUpdate = record.getStatusUpdate();
        if (isStartedUpdateAndStartOfRun(statusUpdate)) {
            ProcessRun.Builder builder = ProcessRun.builder()
                    .startedStatus((ProcessRunStartedUpdate) statusUpdate)
                    .taskId(taskId);
            if (jobRunId != null) {
                builderByJobRunId.put(jobRunId, builder);
            } else {
                builderByTaskId.put(taskId, builder);
            }
            orderedBuilders.add(builder);
        } else if (builderByJobRunId.containsKey(jobRunId)) {
            addToBuilderByKey(statusUpdate, builderByJobRunId, jobRunId)
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
        if (statusUpdate instanceof ProcessRunFinishedUpdate) {
            return Optional.of(builderMap.remove(key)
                    .finishedStatus((ProcessRunFinishedUpdate) statusUpdate));
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
