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

package sleeper.status.report.compactionjob;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class CompactionJobStatusCollector {
    private final CompactionJobStatusStore compactionJobStatusStore;
    private final String tableName;

    public CompactionJobStatusCollector(CompactionJobStatusStore compactionJobStatusStore, String tableName) {
        this.compactionJobStatusStore = compactionJobStatusStore;
        this.tableName = tableName;
    }

    public List<CompactionJobStatus> runUnfinishedQuery() {
        return compactionJobStatusStore.getUnfinishedJobs(tableName);
    }

    public List<CompactionJobStatus> runRangeQuery(Instant startRange, Instant endRange) {
        return compactionJobStatusStore.getJobsInTimePeriod(tableName, startRange, endRange);
    }

    public List<CompactionJobStatus> runDetailedQuery(List<String> jobIds) {
        return jobIds.stream().map(compactionJobStatusStore::getJob).collect(Collectors.toList());
    }

    public List<CompactionJobStatus> runAllQuery() {
        return compactionJobStatusStore.getAllJobs(tableName);
    }
}
