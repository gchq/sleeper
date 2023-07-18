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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;

import java.util.function.Consumer;

import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTableProperties;
import static sleeper.configuration.properties.table.TableProperty.DATA_BUCKET;

public class DynamoDBStateStoreTestHelper {

    private DynamoDBStateStoreTestHelper() {
    }

    public static TableProperties createTestTable(
            InstanceProperties instanceProperties, Schema schema, AmazonS3 s3, AmazonDynamoDB dynamoDB,
            Consumer<TableProperties> tableConfig) {

        TableProperties tableProperties = createTestTableProperties(instanceProperties, schema, s3, tableConfig);
        s3.createBucket(tableProperties.get(DATA_BUCKET));
        try {
            new DynamoDBStateStoreCreator(instanceProperties, tableProperties, dynamoDB).create();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create state store", e);
        }
        return tableProperties;
    }
}
