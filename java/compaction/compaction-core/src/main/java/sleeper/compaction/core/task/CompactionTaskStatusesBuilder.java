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
package sleeper.compaction.core.task;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompactionTaskStatusesBuilder {
    private final Map<String, CompactionTaskStatus.Builder> builderById = new HashMap<>();

    public CompactionTaskStatusesBuilder taskStarted(
            String taskId, Instant startTime, Instant expiryDate) {
        builderById.computeIfAbsent(taskId,
                id -> CompactionTaskStatus.builder().taskId(id))
                .startTime(startTime).expiryDate(expiryDate);
        return this;
    }

    public CompactionTaskStatusesBuilder taskFinished(
            String taskId, CompactionTaskFinishedStatus finishedStatus) {
        Optional.ofNullable(builderById.get(taskId))
                .ifPresent(builder -> builder.finishedStatus(finishedStatus));
        return this;
    }

    public Stream<CompactionTaskStatus> stream() {
        return builderById.values().stream()
                .map(CompactionTaskStatus.Builder::build)
                .sorted(Comparator.comparing(CompactionTaskStatus::getStartTime).reversed());
    }

    public List<CompactionTaskStatus> build() {
        return stream().collect(Collectors.toList());
    }

}
