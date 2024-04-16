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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class JobStatusesBuilder {

    private final Map<String, List<ProcessStatusUpdateRecord>> updatesByJobId = new HashMap<>();

    public JobStatusesBuilder update(ProcessStatusUpdateRecord update) {
        updatesByJobId.computeIfAbsent(update.getJobId(), id -> new ArrayList<>())
                .add(update);
        return this;
    }

    public Stream<JobStatusUpdates> stream() {
        return updatesByJobId.entrySet().stream()
                .map(entry -> JobStatusUpdates.from(entry.getKey(), entry.getValue()))
                .sorted(latestFirstByFirstUpdate());
    }

    private static Comparator<JobStatusUpdates> latestFirstByFirstUpdate() {
        return Comparator.comparing((JobStatusUpdates job) -> job.getFirstRecord().getUpdateTime()).reversed();
    }
}
