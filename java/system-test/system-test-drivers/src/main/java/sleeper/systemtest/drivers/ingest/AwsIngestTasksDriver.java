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

package sleeper.systemtest.drivers.ingest;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.tracker.job.IngestJobTrackerFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.IngestTasksDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.WaitForTasks;

public class AwsIngestTasksDriver implements IngestTasksDriver {

    private final SystemTestInstanceContext instance;
    private final DynamoDbClient dynamoClient;

    public AwsIngestTasksDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.dynamoClient = clients.getDynamo();
    }

    @Override
    public WaitForTasks waitForTasksForCurrentInstance() {
        InstanceProperties instanceProperties = instance.getInstanceProperties();
        IngestJobTracker jobTracker = IngestJobTrackerFactory.getTracker(dynamoClient, instanceProperties);
        return new WaitForTasks(jobTracker);
    }
}
