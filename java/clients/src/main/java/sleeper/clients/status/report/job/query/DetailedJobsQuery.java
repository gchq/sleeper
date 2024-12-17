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
package sleeper.clients.status.report.job.query;

import sleeper.compaction.core.job.CompactionJobStatusStore;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.ingest.core.job.status.IngestJobStatus;
import sleeper.ingest.core.job.status.IngestJobStatusStore;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DetailedJobsQuery implements JobQuery {

    private final List<String> jobIds;

    public DetailedJobsQuery(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobStatusStore statusStore) {
        return run(statusStore::getJob);
    }

    @Override
    public List<IngestJobStatus> run(IngestJobStatusStore statusStore) {
        return run(statusStore::getJob);
    }

    @Override
    public Type getType() {
        return Type.DETAILED;
    }

    private <T> List<T> run(Function<String, Optional<T>> getJob) {
        return jobIds.stream()
                .map(getJob)
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toList());
    }

    public static JobQuery fromParameters(String queryParameters) {
        if ("".equals(queryParameters)) {
            return null;
        }
        return new DetailedJobsQuery(Arrays.asList(queryParameters.split(",")));
    }

}
