/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.clients.report.job.query;

import sleeper.core.table.TableStatus;
import sleeper.core.tracker.compaction.job.CompactionJobTracker;
import sleeper.core.tracker.compaction.job.query.CompactionJobStatus;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.core.tracker.ingest.job.query.IngestJobStatus;

import java.util.List;

/**
 * A query to generate a report on all jobs held in a job tracker.
 */
public class AllJobsQuery implements JobQuery {
    private final String tableId;

    public AllJobsQuery(TableStatus table) {
        this.tableId = table.getTableUniqueId();
    }

    @Override
    public List<CompactionJobStatus> run(CompactionJobTracker tracker) {
        return tracker.getAllJobs(tableId);
    }

    @Override
    public List<IngestJobStatus> run(IngestJobTracker tracker) {
        return tracker.getAllJobs(tableId);
    }

    @Override
    public Type getType() {
        return Type.ALL;
    }
}
