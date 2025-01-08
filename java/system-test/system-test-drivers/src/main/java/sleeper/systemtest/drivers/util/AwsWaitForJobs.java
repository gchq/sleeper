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

package sleeper.systemtest.drivers.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import sleeper.compaction.tracker.job.CompactionJobTrackerFactory;
import sleeper.compaction.tracker.task.CompactionTaskTrackerFactory;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.ingest.tracker.task.IngestTaskTrackerFactory;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;
import sleeper.systemtest.dsl.util.WaitForJobs;

public class AwsWaitForJobs {

    private AwsWaitForJobs() {
    }

    public static WaitForJobs forIngest(SystemTestInstanceContext instance, AmazonDynamoDB dynamoDBClient, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forIngest(instance,
                properties -> IngestJobTrackerFactory.getTracker(dynamoDBClient, properties),
                properties -> IngestTaskTrackerFactory.getTracker(dynamoDBClient, properties),
                pollDriver);
    }

    public static WaitForJobs forBulkImport(SystemTestInstanceContext instance, AmazonDynamoDB dynamoDBClient, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forBulkImport(instance,
                properties -> IngestJobTrackerFactory.getTracker(dynamoDBClient, properties),
                pollDriver);
    }

    public static WaitForJobs forCompaction(SystemTestInstanceContext instance, AmazonDynamoDB dynamoDBClient, PollWithRetriesDriver pollDriver) {
        return WaitForJobs.forCompaction(instance,
                properties -> CompactionJobTrackerFactory.getTracker(dynamoDBClient, properties),
                properties -> CompactionTaskTrackerFactory.getTracker(dynamoDBClient, properties),
                pollDriver);
    }
}
