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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.dynamodb.tools.DynamoDBUtils.loadPagedItemsWithLimit;

public class DynamoDBUtilsPagingLimitIT extends DynamoDBToolsTestBase {

    private final String tableName = UUID.randomUUID().toString();

    @Nested
    @DisplayName("Running Scan")
    class RunningScan {
        @BeforeEach
        void setup() {
            initialiseTable(dynamoClient, tableName,
                    List.of(
                            AttributeDefinition.builder()
                                    .attributeName(TEST_KEY)
                                    .attributeType(ScalarAttributeType.S)
                                    .build()),
                    List.of(
                            KeySchemaElement.builder().attributeName(TEST_KEY)
                                    .keyType(KeyType.HASH).build()));
        }

        @Test
        void shouldLoadRecordsMatchingLoadLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key1")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key2")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 2, scanWithLimit(tableName, 2)))
                    .satisfies(items -> {
                        assertThat(items.getItems()).containsExactlyInAnyOrder(record1, record2);
                        assertThat(items.isMoreItems()).isFalse();
                    });
        }

        @Test
        void shouldLoadRecordsBelowLoadLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key1")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key2")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 10, scan()))
                    .satisfies(items -> {
                        assertThat(items.getItems()).containsExactlyInAnyOrder(record1, record2);
                        assertThat(items.isMoreItems()).isFalse();
                    });
        }

        @Test
        void shouldLoadRecordsWhenLoadLimitIsMetOnFirstPage() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key1")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key2")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 1, scanWithLimit(tableName, 10)))
                    .satisfies(items -> {
                        assertThat(items.getItems())
                                .hasSize(1)
                                .isSubsetOf(record1, record2);
                        assertThat(items.isMoreItems()).isTrue();
                    });
        }

        @Test
        void shouldLoadRecordsWhenLoadLimitIsMetOnSecondPage() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key1")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key2")
                    .string(TEST_VALUE, "value2").build();
            Map<String, AttributeValue> record3 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key3")
                    .string(TEST_VALUE, "value3").build();
            Map<String, AttributeValue> record4 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key4")
                    .string(TEST_VALUE, "value4").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));
            dynamoClient.putItem(buildPutItemRequest(tableName, record3));
            dynamoClient.putItem(buildPutItemRequest(tableName, record4));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 3, scanWithLimit(tableName, 2)))
                    .satisfies(items -> {
                        assertThat(items.getItems())
                                .hasSize(3)
                                .isSubsetOf(record1, record2, record3, record4);
                        assertThat(items.isMoreItems()).isTrue();
                    });
        }

        @Test
        void shouldLoadRecordsWhenLoadLimitIsMetAtEndOfFirstPageAndAnotherPageIsAvailable() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key1")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "key2")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 1, scanWithLimit(tableName, 1)))
                    .satisfies(items -> {
                        assertThat(items.getItems())
                                .hasSize(1)
                                .isSubsetOf(record1, record2);
                        assertThat(items.isMoreItems()).isTrue();
                    });
        }
    }

    @Nested
    @DisplayName("Running Query")
    class RunningQuery {
        @BeforeEach
        void setUp() {
            initialiseTable(dynamoClient, tableName,
                    List.of(
                            AttributeDefinition.builder().attributeName(TEST_KEY).attributeType(ScalarAttributeType.S).build(),
                            AttributeDefinition.builder().attributeName(TEST_VALUE).attributeType(ScalarAttributeType.S).build()),
                    List.of(
                            KeySchemaElement.builder().attributeName(TEST_KEY).keyType(KeyType.HASH).build(),
                            KeySchemaElement.builder().attributeName(TEST_VALUE).keyType(KeyType.RANGE).build()));
        }

        @Test
        void shouldLoadRecordsMatchingLoadLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 2, queryForKeyWithLimit(tableName, "test-key", 2)))
                    .satisfies(items -> {
                        assertThat(items.getItems()).containsExactlyInAnyOrder(record1, record2);
                        assertThat(items.isMoreItems()).isFalse();
                    });
        }

        @Test
        void shouldLoadRecordsBelowLoadLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 10, queryForKeyWithLimit(tableName, "test-key", 2)))
                    .satisfies(items -> {
                        assertThat(items.getItems()).containsExactlyInAnyOrder(record1, record2);
                        assertThat(items.isMoreItems()).isFalse();
                    });
        }

        @Test
        void shouldLoadRecordsWhenLoadLimitIsMetOnFirstPage() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 1, queryForKeyWithLimit(tableName, "test-key", 10)))
                    .satisfies(items -> {
                        assertThat(items.getItems())
                                .hasSize(1)
                                .isSubsetOf(record1, record2);
                        assertThat(items.isMoreItems()).isTrue();
                    });
        }

        @Test
        void shouldLoadRecordsWhenLoadLimitIsMetOnSecondPage() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value2").build();
            Map<String, AttributeValue> record3 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value3").build();
            Map<String, AttributeValue> record4 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value4").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));
            dynamoClient.putItem(buildPutItemRequest(tableName, record3));
            dynamoClient.putItem(buildPutItemRequest(tableName, record4));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 3, queryForKeyWithLimit(tableName, "test-key", 2)))
                    .satisfies(items -> {
                        assertThat(items.getItems())
                                .hasSize(3)
                                .isSubsetOf(record1, record2, record3, record4);
                        assertThat(items.isMoreItems()).isTrue();
                    });
        }

        @Test
        void shouldLoadRecordsWhenLoadLimitIsMetAtEndOfFirstPageAndAnotherPageIsAvailable() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value1").build();
            Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, "test-key")
                    .string(TEST_VALUE, "value2").build();

            dynamoClient.putItem(buildPutItemRequest(tableName, record1));
            dynamoClient.putItem(buildPutItemRequest(tableName, record2));

            // When/Then
            assertThat(loadPagedItemsWithLimit(dynamoClient, 1, queryForKeyWithLimit(tableName, "test-key", 1)))
                    .satisfies(items -> {
                        assertThat(items.getItems())
                                .hasSize(1)
                                .isSubsetOf(record1, record2);
                        assertThat(items.isMoreItems()).isTrue();
                    });
        }
    }

    private ScanRequest scan() {
        return ScanRequest.builder()
                .tableName(tableName)
                .build();
    }
}
