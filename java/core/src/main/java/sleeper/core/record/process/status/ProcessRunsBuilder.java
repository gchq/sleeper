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
import java.util.stream.Collectors;

class ProcessRunsBuilder {
    private final Map<String, ProcessRun.Builder> taskBuilders = new HashMap<>();
    private final List<ProcessRun.Builder> orderedBuilders = new ArrayList<>();

    void add(ProcessStatusUpdateRecord record) {
        String taskId = record.getTaskId();
        ProcessStatusUpdate statusUpdate = record.getStatusUpdate();
        if (isStartedUpdateAndStartOfRun(statusUpdate)) {
            ProcessRun.Builder builder = ProcessRun.builder()
                    .startedStatus(((ProcessRunStartedUpdate) statusUpdate))
                    .taskId(taskId);
            taskBuilders.put(taskId, builder);
            orderedBuilders.add(builder);
        } else if (statusUpdate.isPartOfRun() && taskBuilders.containsKey(taskId)) {
            if (statusUpdate instanceof ProcessFinishedStatus) {
                taskBuilders.remove(taskId)
                        .finishedStatus((ProcessFinishedStatus) statusUpdate)
                        .taskId(taskId);
            } else {
                taskBuilders.get(taskId)
                        .statusUpdate(statusUpdate);
            }
        }
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
