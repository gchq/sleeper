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
package sleeper.core.tracker.job.run;

import sleeper.core.tracker.job.status.JobStatusUpdateRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Gathers job tracker records and correlates which updates occurred in the same run of a job. Creates a
 * {@link JobRuns} object with the detected runs.
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
 * in the resulting {@link JobRuns} object, and must be handled separately.
 */
class JobRunsBuilder {
    private final Map<String, JobRun.Builder> builderByJobRunId = new HashMap<>();
    private final List<JobRun.Builder> orderedBuilders = new ArrayList<>();

    void add(JobStatusUpdateRecord record) {
        if (!record.getStatusUpdate().isPartOfRun()) {
            return;
        }
        getBuilderIfCorrelatable(record)
                .ifPresent(builder -> {
                    builder.statusUpdate(record.getStatusUpdate());
                    if (record.getTaskId() != null) {
                        builder.taskId(record.getTaskId());
                    }
                });
    }

    private Optional<JobRun.Builder> getBuilderIfCorrelatable(JobStatusUpdateRecord record) {
        if (record.getStatusUpdate().isPartOfRun()) {
            return Optional.of(builderByJobRunId.computeIfAbsent(record.getJobRunId(), id -> createOrderedBuilder()));
        } else {
            return Optional.empty();
        }
    }

    private JobRun.Builder createOrderedBuilder() {
        JobRun.Builder builder = JobRun.builder();
        orderedBuilders.add(builder);
        return builder;
    }

    JobRuns build() {
        List<JobRun> jobRuns = orderedBuilders.stream()
                .map(JobRun.Builder::build)
                .collect(Collectors.toList());
        Collections.reverse(jobRuns);
        return JobRuns.latestFirst(jobRuns);
    }
}
