/*
 * Copyright 2022-2026 Crown Copyright
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
import software.amazon.awssdk.services.sts.StsClient;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.TABLE_ONLINE;

public class TakeTableOffline {
    private static final Logger LOGGER = LoggerFactory.getLogger(TakeTableOffline.class);

    private final TablePropertiesStore tablePropertiesStore;

    public TakeTableOffline(TablePropertiesStore tablePropertiesStore) {
        this.tablePropertiesStore = tablePropertiesStore;
    }

    public void takeOffline(String tableName) {
        TableProperties tableProperties = tablePropertiesStore.loadByNameNoValidation(tableName);
        tableProperties.set(TABLE_ONLINE, "false");
        tablePropertiesStore.save(tableProperties);
        LOGGER.info("Successfully took table offline: {}", tableProperties.getStatus());
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <instance-id> <table-name>");
        }
        String instanceId = args[0];
        String tableName = args[1];

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder());
                StsClient stsClient = buildAwsV2Client(StsClient.builder())) {
            String accountName = stsClient.getCallerIdentity().account();
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceIdAndAccount(s3Client, instanceId, accountName);
            TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
            new TakeTableOffline(tablePropertiesStore).takeOffline(tableName);
        }
    }
}
