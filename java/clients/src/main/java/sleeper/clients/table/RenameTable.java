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
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesStore;

import static sleeper.configuration.utils.AwsV2ClientHelper.buildAwsV2Client;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class RenameTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenameTable.class);

    private final TablePropertiesStore tablePropertiesStore;

    public RenameTable(TablePropertiesStore tablePropertiesStore) {
        this.tablePropertiesStore = tablePropertiesStore;
    }

    public void rename(String oldName, String newName) {
        TableProperties tableProperties = tablePropertiesStore.loadByName(oldName);
        tableProperties.set(TABLE_NAME, newName);
        tablePropertiesStore.save(tableProperties);
        LOGGER.info("Successfully renamed table from {} to {}", oldName, newName);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: <instance-id> <old-table-name> <new-table-name>");
        }

        try (S3Client s3Client = buildAwsV2Client(S3Client.builder());
                DynamoDbClient dynamoClient = buildAwsV2Client(DynamoDbClient.builder())) {
            InstanceProperties instanceProperties = S3InstanceProperties.loadGivenInstanceId(s3Client, args[0]);
            TablePropertiesStore tablePropertiesStore = S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
            new RenameTable(tablePropertiesStore).rename(args[1], args[2]);
        }
    }
}
