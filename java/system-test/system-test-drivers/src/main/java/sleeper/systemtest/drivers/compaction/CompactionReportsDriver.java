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

package sleeper.systemtest.drivers.compaction;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.clients.status.report.CompactionJobStatusReport;
import sleeper.clients.status.report.CompactionTaskStatusReport;
import sleeper.clients.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.clients.status.report.compaction.task.CompactionTaskQuery;
import sleeper.clients.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.clients.status.report.job.query.JobQuery;
import sleeper.clients.status.report.job.query.RangeJobsQuery;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.status.store.job.CompactionJobStatusStoreFactory;
import sleeper.compaction.status.store.task.CompactionTaskStatusStoreFactory;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestReport;

import java.time.Instant;

public class CompactionReportsDriver {
    private final CompactionTaskStatusStore compactionTaskStatusStore;
    private final CompactionJobStatusStore compactionJobStatusStore;
    private final SleeperInstanceContext instance;

    public CompactionReportsDriver(AmazonDynamoDB dynamoDB, SleeperInstanceContext instance) {
        InstanceProperties properties = instance.getInstanceProperties();
        this.compactionTaskStatusStore = CompactionTaskStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.compactionJobStatusStore = CompactionJobStatusStoreFactory.getStatusStore(dynamoDB, properties);
        this.instance = instance;
    }

    public SystemTestReport tasksAndJobsReport() {
        return (out, startTime) -> {
            new CompactionTaskStatusReport(compactionTaskStatusStore,
                    new StandardCompactionTaskStatusReporter(out),
                    CompactionTaskQuery.forPeriod(startTime, Instant.MAX))
                    .run();
            new CompactionJobStatusReport(compactionJobStatusStore,
                    new StandardCompactionJobStatusReporter(out),
                    JobQuery.Type.RANGE, new RangeJobsQuery(instance.getTableName(), startTime, Instant.MAX))
                    .run();
        };
    }
}
