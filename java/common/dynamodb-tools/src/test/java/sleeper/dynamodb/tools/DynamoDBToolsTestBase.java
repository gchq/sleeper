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

package sleeper.dynamodb.tools;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.Map;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

public class DynamoDBToolsTestBase extends LocalStackTestBase {
    public static final String TEST_KEY = "test-key";
    public static final String TEST_VALUE = "test-value";

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
