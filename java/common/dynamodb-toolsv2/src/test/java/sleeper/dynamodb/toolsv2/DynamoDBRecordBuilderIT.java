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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.initialiseTable;

class DynamoDBRecordBuilderIT extends DynamoDBToolsTestBase {

    private final String tableName = UUID.randomUUID().toString();

    @BeforeEach
    void setup() {
        initialiseTable(dynamoClientV2, tableName,
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

    @Test
    void shouldCreateRecordWithStringAttribute() {
        // Given
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .string(TEST_VALUE, "value").build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(record);
    }

    @Test
    void shouldCreateRecordWithIntAttribute() {
        // Given
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .number(TEST_VALUE, 123).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(record);
    }

    @Test
    void shouldCreateRecordWithLongAttribute() {
        // Given
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .number(TEST_VALUE, 123L).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(record);
    }

    @Test
    void shouldCreateRecordWithInstantAttribute() {
        // Given
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .number(TEST_VALUE, Instant.now().toEpochMilli()).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(record);
    }

    @Test
    void shouldCreateRecordWithNaN() {
        // Given
        String key = UUID.randomUUID().toString();
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, key)
                .number(TEST_VALUE, Double.NaN).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(record);
    }

    @Test
    void shouldCreateRecordWithNullNumber() {
        // Given
        String key = UUID.randomUUID().toString();
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, key)
                .number(TEST_VALUE, null).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(record);
    }

    @Test
    void shouldUnsetExistingNumberAttributeWhenProvidingNull() {
        // Given
        String key = UUID.randomUUID().toString();
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, key)
                .number(TEST_VALUE, 123)
                .number(TEST_VALUE, null).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(new DynamoDBRecordBuilder()
                .string(TEST_KEY, key).build());
    }

    @Test
    void shouldUnsetExistingStringAttributeWhenProvidingNull() {
        // Given
        String key = UUID.randomUUID().toString();
        Map<String, AttributeValue> record = new DynamoDBRecordBuilder()
                .string(TEST_KEY, key)
                .string(TEST_VALUE, "abc")
                .string(TEST_VALUE, null).build();
        // When
        dynamoClientV2.putItem(buildPutItemRequest(tableName, record));
        // Then
        ScanResponse result = dynamoClientV2.scan(ScanRequest.builder().tableName(tableName).build());
        assertThat(result.items()).containsExactly(new DynamoDBRecordBuilder()
                .string(TEST_KEY, key).build());
    }
}
