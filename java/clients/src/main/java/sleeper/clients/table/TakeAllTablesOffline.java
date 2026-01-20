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

package sleeper.clients.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesStore;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

public class TakeAllTablesOffline {
    private static final Logger LOGGER = LoggerFactory.getLogger(TakeAllTablesOffline.class);

    private S3Client s3Client;
    private DynamoDbClient dynamoClient;

    public TakeAllTablesOffline(S3Client s3Client, DynamoDbClient dynamoClient) {
        this.s3Client = s3Client;
        this.dynamoClient = dynamoClient;
    }

    public void takeAllOffline(String instanceId) {
        InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, instanceId);
        takeAllOffline(instanceProperties);
    }

    public void takeAllOffline(InstanceProperties instanceProperties) {
        TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
        String instanceId = instanceProperties.get(ID);

        tablePropertiesStore.streamOnlineTables().forEach(tableProperties -> {
            String tableName = tableProperties.get(TABLE_NAME);
            tableProperties.set(TABLE_ONLINE, "false");
            tablePropertiesStore.save(tableProperties);
            LOGGER.info("Successfully took table offline {}:{}", instanceId, tableName);
        });
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: <instanceId> [<instanceId>...]");
        }

        try (
            S3Client s3Client = buildAwsV2Client(S3Client.builder());
            DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())
        ) {
            TakeAllTablesOffline offliner = new TakeAllTablesOffline(s3Client, dynamoClient);
            for (int i = 0; i < args.length; i++) {
                offliner.takeAllOffline(args[i]);
            }
        }
    }

}
