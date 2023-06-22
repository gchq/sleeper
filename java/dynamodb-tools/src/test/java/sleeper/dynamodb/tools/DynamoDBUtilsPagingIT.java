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

package sleeper.dynamodb.tools;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.dynamodb.tools.DynamoDBUtils.streamPagedItems;

public class DynamoDBUtilsPagingIT extends DynamoDBTableTestBase {
    @Test
    void shouldReturnPagedResultsWhenMoreRecordsThanScanLimit() {
        // Given
        Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .string(TEST_VALUE, "value1").build();
        Map<String, AttributeValue> record2 = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .string(TEST_VALUE, "value2").build();

        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record1));
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record2));

        // When/Then
        assertThat(streamPagedItems(dynamoDBClient, new ScanRequest()
                .withTableName(TEST_TABLE_NAME)
                .withLimit(1)))
                .containsExactlyInAnyOrder(record1, record2);
    }

    @Test
    void shouldReturnOnePageOfResultsWhenFewerRecordsThanScanLimit() {
        // Given
        Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .string(TEST_VALUE, "value1").build();

        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record1));

        // When/Then
        assertThat(streamPagedItems(dynamoDBClient, new ScanRequest()
                .withTableName(TEST_TABLE_NAME)
                .withLimit(2)))
                .containsExactlyInAnyOrder(record1);
    }

    @Test
    void shouldReturnPagedResultsWhenRecordsEqualToScanLimit() {
        // Given
        Map<String, AttributeValue> record1 = new DynamoDBRecordBuilder()
                .string(TEST_KEY, UUID.randomUUID().toString())
                .string(TEST_VALUE, "value1").build();

        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record1));

        // When/Then
        assertThat(streamPagedItems(dynamoDBClient, new ScanRequest()
                .withTableName(TEST_TABLE_NAME)
                .withLimit(1)))
                .containsExactlyInAnyOrder(record1);
    }

    @Test
    void shouldReturnPagedResultsWhenLastPageHasFewerRecordsThanScanLimit() {
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

        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record1));
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record2));
        dynamoDBClient.putItem(new PutItemRequest(TEST_TABLE_NAME, record3));

        // When/Then
        assertThat(streamPagedItems(dynamoDBClient, new ScanRequest()
                .withTableName(TEST_TABLE_NAME)
                .withLimit(2)))
                .containsExactlyInAnyOrder(record1, record2, record3);
    }

    @Test
    void shouldReturnNoResultsWhenNoRecordsExist() {
        // When/Then
        assertThat(streamPagedItems(dynamoDBClient, new ScanRequest()
                .withTableName(TEST_TABLE_NAME)
                .withLimit(1)))
                .isEmpty();
    }
}
