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

package sleeper.status.report;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.statestore.StateStoreException;
import sleeper.status.report.compactionjob.CompactionJobStatusCollector;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;
import sleeper.status.report.compactionjob.JsonCompactionJobStatusReporter;
import sleeper.status.report.compactionjob.StandardCompactionJobStatusReporter;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class CompactionJobStatusReport {
    private final CompactionJobStatusReporter compactionJobStatusReporter;
    private final CompactionJobStatusCollector compactionJobStatusCollector;
    private static final String DEFAULT_STATUS_REPORTER = "STANDARD";
    private static final Map<String, CompactionJobStatusReporter> FILE_STATUS_REPORTERS = new HashMap<>();
    private static final Instant DEFAULT_RANGE_START = Instant.now().minus(4L, ChronoUnit.HOURS);
    private static final Instant DEFAULT_RANGE_END = Instant.now();

    static {
        FILE_STATUS_REPORTERS.put(DEFAULT_STATUS_REPORTER, new StandardCompactionJobStatusReporter());
        FILE_STATUS_REPORTERS.put("JSON", new JsonCompactionJobStatusReporter());
    }

    public CompactionJobStatusReport(AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties) {
        String instanceId = instanceProperties.get(ID);
        String tableName = DynamoDBCompactionJobStatusStore.jobStatusTableName(instanceId);
        CompactionJobStatusStore compactionJobStatusStore = DynamoDBCompactionJobStatusStore.from(dynamoDB, instanceProperties);
        compactionJobStatusCollector = new CompactionJobStatusCollector(compactionJobStatusStore, tableName);
        compactionJobStatusReporter = FILE_STATUS_REPORTERS.get(DEFAULT_STATUS_REPORTER);
    }

    public void handleUnfinishedQuery() {
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runUnfinishedQuery();
        compactionJobStatusReporter.report(statusList, QueryType.UNFINISHED);
    }

    public void handleRangeQuery() {
        Instant startTime = DEFAULT_RANGE_START;
        Instant endTime = DEFAULT_RANGE_END;
        // TODO loop and prompt for start/end range
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runRangeQuery(startTime, endTime);
        compactionJobStatusReporter.report(statusList, QueryType.RANGE);
    }

    public void handleDetailedQuery() {
        List<String> jobIds = Collections.emptyList();
        // TODO loop and prompt for jobIds
        List<CompactionJobStatus> statusList = compactionJobStatusCollector.runDetailedQuery(jobIds);
        compactionJobStatusReporter.report(statusList, QueryType.DETAILED);
    }

    public static void main(String[] args) throws IOException, StateStoreException {
        if (!(args.length >= 2 && args.length <= 5)) {
            throw new IllegalArgumentException("Usage: <instance id> <optional_report_type_standard_or_csv_or_json>");
        }
    }
}
