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

package sleeper.statestore.s3;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import sleeper.configuration.properties.table.TableProperties;

import java.util.List;

import static sleeper.configuration.properties.table.TableProperty.REVISION_TABLENAME;

public class S3StateStoreCreator {
    private final AmazonDynamoDB dynamoDB;
    private final TableProperties tableProperties;

    public S3StateStoreCreator(TableProperties tableProperties, AmazonDynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
        this.tableProperties = tableProperties;
    }

    public void create() {
        String tableName = tableProperties.get(REVISION_TABLENAME);
        List<AttributeDefinition> attributeDefinitions = List.of(
                new AttributeDefinition(S3StateStore.REVISION_ID_KEY, ScalarAttributeType.S));
        List<KeySchemaElement> keySchemaElements = List.of(
                new KeySchemaElement(S3StateStore.REVISION_ID_KEY, KeyType.HASH));
        CreateTableRequest request = new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(keySchemaElements)
                .withBillingMode(BillingMode.PAY_PER_REQUEST);
        dynamoDB.createTable(request);
    }
}
