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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessStatusesBuilder {
    private final Map<String, List<ProcessStatusUpdateRecord>> runUpdatesByJobId = new HashMap<>();

    public ProcessStatusesBuilder processUpdate(ProcessStatusUpdateRecord processUpdate) {
        runUpdatesByJobId.computeIfAbsent(processUpdate.getJobId(), id -> new ArrayList<>())
                .add(processUpdate);
        return this;
    }

    public List<ProcessRun> buildRunList(List<ProcessStatusUpdateRecord> recordList) {
        Map<String, ProcessRun.Builder> taskBuilders = new HashMap<>();
        List<ProcessRun.Builder> orderedBuilders = new ArrayList<>();
        for (ProcessStatusUpdateRecord record : recordList) {
            String taskId = record.getTaskId();
            ProcessStatusUpdate statusUpdate = record.getStatusUpdate();
            if (statusUpdate instanceof ProcessStartedStatus) {
                ProcessRun.Builder builder = ProcessRun.builder()
                        .startedStatus((ProcessStartedStatus) statusUpdate)
                        .taskId(taskId);
                taskBuilders.put(taskId, builder);
                orderedBuilders.add(builder);
            } else {
                taskBuilders.remove(taskId)
                        .finishedStatus((ProcessFinishedStatus) statusUpdate)
                        .taskId(taskId);
            }
        }
        List<ProcessRun> jobRuns = orderedBuilders.stream()
                .map(ProcessRun.Builder::build)
                .collect(Collectors.toList());
        Collections.reverse(jobRuns);
        return jobRuns;
    }

    public List<ProcessStatusUpdateRecord> runUpdatesOrderedByUpdateTime(String jobId) {
        return runUpdatesByJobId.getOrDefault(jobId, Collections.emptyList())
                .stream().sorted(Comparator.comparing(update -> update.getStatusUpdate().getUpdateTime()))
                .collect(Collectors.toList());
    }

}
