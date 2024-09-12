/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.dynamodb.test.DynamoDBTestBase;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;
import static sleeper.dynamodb.tools.DynamoDBTableTestBase.TEST_KEY;
import static sleeper.dynamodb.tools.DynamoDBTableTestBase.TEST_VALUE;
import static sleeper.dynamodb.tools.DynamoDBUtils.initialiseTable;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedResults;

public class DynamoDBUtilsPagingIT extends DynamoDBTestBase {

    private final String tableName = UUID.randomUUID().toString();

    @AfterEach
    public void tearDown() {
        dynamoDBClient.deleteTable(tableName);
    }

    @Nested
    @DisplayName("Running Scan")
    class RunningScan {
        @BeforeEach
        void setup() {
            initialiseTable(dynamoDBClient, tableName,
                    List.of(
                            new AttributeDefinition(TEST_KEY, ScalarAttributeType.S)),
                    List.of(
                            new KeySchemaElement(TEST_KEY, KeyType.HASH)));
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, scan().withLimit(1)))
                    .containsExactlyInAnyOrder(record1, record2);
        }

        @Test
        void shouldReturnOnePageOfResultsWhenFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, scan().withLimit(2)))
                    .containsExactlyInAnyOrder(record1);
        }

        @Test
        void shouldReturnPagedResultsWhenRecordsEqualToLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, scan().withLimit(1)))
                    .containsExactlyInAnyOrder(record1);
        }

        @Test
        void shouldNotReturnEmptyResultWhenFewerRecordsThanLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedResults(dynamoDBClient, scan().withLimit(2)))
                    .extracting(result -> result.getItems().size())
                    .containsExactly(1);
        }

        @Test
        void shouldReturnEmptyResultForLastPageWhenRecordsEqualToLimit() {
            // Given
            Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                    .string(TEST_KEY, UUID.randomUUID().toString())
                    .string(TEST_VALUE, "value1").build();

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));

            // When/Then
            assertThat(streamPagedResults(dynamoDBClient, scan().withLimit(1)))
                    .extracting(result -> result.getItems().size())
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record3));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, scan().withLimit(2)))
                    .containsExactlyInAnyOrder(record1, record2, record3);
        }

        @Test
        void shouldReturnNoResultsWhenNoRecordsExist() {
            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, scan().withLimit(1)))
                    .isEmpty();
        }
    }

    @Nested
    @DisplayName("Running Query")
    class RunningQuery {
        @BeforeEach
        void setUp() {
            initialiseTable(dynamoDBClient, tableName,
                    List.of(
                            new AttributeDefinition(TEST_KEY, ScalarAttributeType.S),
                            new AttributeDefinition(TEST_VALUE, ScalarAttributeType.S)),
                    List.of(
                            new KeySchemaElement(TEST_KEY, KeyType.HASH),
                            new KeySchemaElement(TEST_VALUE, KeyType.RANGE)));
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, queryForKey("test-key").withLimit(1)))
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, queryForKey("test-key").withLimit(3)))
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, queryForKey("test-key").withLimit(2)))
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedResults(dynamoDBClient, queryForKey("test-key").withLimit(3)))
                    .extracting(result -> result.getItems().size())
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));

            // When/Then
            assertThat(streamPagedResults(dynamoDBClient, queryForKey("test-key").withLimit(2)))
                    .extracting(result -> result.getItems().size())
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

            dynamoDBClient.putItem(new PutItemRequest(tableName, record1));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record2));
            dynamoDBClient.putItem(new PutItemRequest(tableName, record3));

            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, queryForKey("test-key").withLimit(4)))
                    .containsExactlyInAnyOrder(record1, record2, record3);
        }

        @Test
        void shouldReturnNoResultsWhenNoRecordsExist() {
            // When/Then
            assertThat(streamPagedItems(dynamoDBClient, queryForKey("not-a-key").withLimit(1)))
                    .isEmpty();
        }
    }

    private ScanRequest scan() {
        return new ScanRequest().withTableName(tableName);
    }

    private QueryRequest queryForKey(String key) {
        return new QueryRequest()
                .withTableName(tableName)
                .withKeyConditionExpression("#TestKey = :testkey")
                .withExpressionAttributeNames(Map.of("#TestKey", TEST_KEY))
                .withExpressionAttributeValues(Map.of(":testkey", createStringAttribute(key)));
    }
}
