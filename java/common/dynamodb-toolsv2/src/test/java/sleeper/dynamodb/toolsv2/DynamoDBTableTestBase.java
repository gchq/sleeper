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

package sleeper.dynamodb.toolsv2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.List;
import java.util.Map;

import static sleeper.dynamodb.toolsv2.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.initialiseTable;

public class DynamoDBTableTestBase extends LocalStackTestBase {
    public static final String TEST_KEY = "test-key";
    public static final String TEST_VALUE = "test-value";
    public static final String TEST_TABLE_NAME = "dynamodb-tools-test-table";

    @BeforeEach
    void setup() {
        createTable();
    }

    @AfterEach
    public void tearDown() {
        dynamoClient.deleteTable(TEST_TABLE_NAME);
    }

    public void createTable() {
        initialiseTable(dynamoClientV2, TEST_TABLE_NAME,
                List.of(
                        AttributeDefinition.builder()
                                .attributeName(TEST_KEY)
                                .attributeType(ScalarAttributeType.S)
                                .build()),
                List.of(
                        KeySchemaElement.builder()
                                .attributeName(TEST_KEY)
                                .keyType(KeyType.HASH)
                                .build()));
    }

    protected PutItemRequest buildPutItemRequest(String tableName, Map<String, AttributeValue> record) {
        return PutItemRequest.builder()
                .tableName(tableName)
                .item(record)
                .build();
    }

    protected ScanRequest scanWithLimit(String tableName, int limit) {
        return ScanRequest.builder()
                .tableName(tableName)
                .limit(limit)
                .build();
    }

    protected QueryRequest queryForKeyWithLimit(String tableName, String key, int limit) {
        return QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#TestKey = :testkey")
                .expressionAttributeNames(Map.of("#TestKey", TEST_KEY))
                .expressionAttributeValues(Map.of(":testkey", createStringAttribute(key)))
                .limit(limit)
                .build();
    }

}
