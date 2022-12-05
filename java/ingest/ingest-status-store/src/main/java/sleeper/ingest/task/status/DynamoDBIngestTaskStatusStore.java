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

package sleeper.ingest.task.status;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.ingest.task.IngestTaskStatusStore;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_STATUS_STORE_ENABLED;
import static sleeper.dynamodb.tools.DynamoDBUtils.instanceTableName;

public class DynamoDBIngestTaskStatusStore implements IngestTaskStatusStore {
    private final AmazonDynamoDB dynamoDB;
    private final String statusTableName;
    private final long timeToLive;

    private DynamoDBIngestTaskStatusStore(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        this.dynamoDB = dynamoDB;
        this.statusTableName = jobStatusTableName(properties.get(ID));
        this.timeToLive = properties.getLong(UserDefinedInstanceProperty.INGEST_TASK_STATUS_TTL_IN_SECONDS) * 1000;
    }

    public static IngestTaskStatusStore from(AmazonDynamoDB dynamoDB, InstanceProperties properties) {
        if (Boolean.TRUE.equals(properties.getBoolean(INGEST_STATUS_STORE_ENABLED))) {
            return new DynamoDBIngestTaskStatusStore(dynamoDB, properties);
        } else {
            return IngestTaskStatusStore.none();
        }
    }

    public static String jobStatusTableName(String instanceId) {
        return instanceTableName(instanceId, "ingest-task-status");
    }

    // Used to prevent spotbugs from failing
    public AmazonDynamoDB getDynamoDB() {
        return dynamoDB;
    }

    // Used to prevent spotbugs from failing
    public String getStatusTableName() {
        return statusTableName;
    }

    // Used to prevent spotbugs from failing
    public long getTimeToLive() {
        return timeToLive;
    }
}
