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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.initialiseTable;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedItems;
import static sleeper.dynamodb.toolsv2.DynamoDBUtils.streamPagedResults;

public class DynamoDBUtilsPagingIT extends DynamoDBToolsTestBase {

    private final String tableName = UUID.randomUUID().toString();

    @Nested
    @DisplayName("Running Scan")
    class RunningScan {
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
        void shouldReturnPagedResultsWhenMoreRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value2").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, scanWithLimit(tableName, 1)))
                    .containsExactlyInAnyOrder(record1, record2);
        }

        @Test
        void shouldReturnOnePageOfResultsWhenFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, scanWithLimit(tableName, 2)))
                    .containsExactlyInAnyOrder(record1);
        }

        @Test
        void shouldReturnPagedResultsWhenRecordsEqualToLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, scanWithLimit(tableName, 1)))
                    .containsExactlyInAnyOrder(record1);
        }

        @Test
        void shouldNotReturnEmptyResultWhenFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedResults(dynamoClientV2, scanWithLimit(tableName, 2)))
                    .extracting(result -> result.items().size())
                    .containsExactly(1);
        }

        @Test
        void shouldReturnEmptyResultForLastPageWhenRecordsEqualToLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedResults(dynamoClientV2, scanWithLimit(tableName, 1)))
                    .extracting(result -> result.items().size())
                    .containsExactly(1, 0);
        }

        @Test
        void shouldReturnPagedResultsWhenLastPageHasFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value2").build();
            Map<String, AttributeValue> record3 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value3").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record3));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, scanWithLimit(tableName, 2)))
                    .containsExactlyInAnyOrder(record1, record2, record3);
        }

        @Test
        void shouldReturnNoResultsWhenNoRecordsExist() {
            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, scanWithLimit(tableName, 1)))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Running Query")
    class RunningQuery {
        @BeforeEach
        void setUp() {
            initialiseTable(dynamoClientV2, tableName,
                    List.of(
                            AttributeDefinition.builder().attributeName(TEST_KEY).attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName(TEST_VALUE).attributeType(ScalarAttributeType.S).build()),
                    List.of(
                            KeySchemaElement.builder().attributeName(TEST_KEY).keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName(TEST_VALUE).keyType(KeyType.RANGE).build()));
        }

        @Test
        void shouldReturnPagedResultsWhenMoreRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-2").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, queryForKeyWithLimit(tableName, "test-key", 1)))
                    .containsExactlyInAnyOrder(record1, record2);
        }

        @Test
        void shouldReturnOnePageOfResultsWhenFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-2").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, queryForKeyWithLimit(tableName, "test-key", 3)))
                    .containsExactlyInAnyOrder(record1, record2);
        }

        @Test
        void shouldReturnPagedResultsWhenRecordsEqualToLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-2").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, queryForKeyWithLimit(tableName, "test-key", 2)))
                    .containsExactlyInAnyOrder(record1, record2);
        }

        @Test
        void shouldNotReturnEmptyResultWhenFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-2").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedResults(dynamoClientV2, queryForKeyWithLimit(tableName, "test-key", 3)))
                    .extracting(result -> result.items().size())
                    .containsExactly(2);
        }

        @Test
        void shouldReturnEmptyResultForLastPageWhenRecordsEqualToLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-2").build();

            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedResults(dynamoClientV2, queryForKeyWithLimit(tableName, "test-key", 2)))
                    .extracting(result -> result.items().size())
                    .containsExactly(2, 0);
        }

        @Test
        void shouldReturnPagedResultsWhenLastPageHasFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-2").build();
            Map<String, AttributeValue> record3 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "test-value-3").build();
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record1));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record2));
            dynamoClientV2.putItem(buildPutItemRequest(tableName, record3));

            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, queryForKeyWithLimit(tableName, "test-key", 4)))
                    .containsExactlyInAnyOrder(record1, record2, record3);
        }

        @Test
        void shouldReturnNoResultsWhenNoRecordsExist() {
            // When/Then
            assertThat(streamPagedItems(dynamoClientV2, queryForKeyWithLimit(tableName, "not-a-key", 1)))
                    .isEmpty();
        }
    }
}
