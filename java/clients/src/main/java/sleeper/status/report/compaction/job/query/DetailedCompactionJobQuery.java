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
package sleeper.status.report.compaction.job.query;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.status.report.compaction.job.CompactionJobQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DetailedCompactionJobQuery implements CompactionJobQuery {

    private final List<String> jobIds;

    public DetailedCompactionJobQuery(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    public static DetailedCompactionJobQuery fromParameters(String queryParameters) {
        if ("".equals(queryParameters)) {
            return null;
        }
        return new DetailedCompactionJobQuery(Arrays.asList(queryParameters.split(",")));
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobStatusStore statusStore) {
        return jobIds.stream()
                .map(statusStore::getJob)
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toList());
    }
}
