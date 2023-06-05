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
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createListAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

class DynamoDBAttributesTest {
    private static final String TEST_KEY = "test-key";

    @Test
    void shouldCreateStringAttribute() {
        // Given we have a string attribute
        AttributeValue value = createStringAttribute("test-value");
        // When we construct a record with a key for the attribute value
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, value);
        // Then the string attribute should be read from the record
        assertThat(DynamoDBAttributes.getStringAttribute(item, TEST_KEY)).isEqualTo("test-value");
    }

    @Test
    void shouldCreateStringAttributeWithNull() {
        assertThat(createStringAttribute(null)).isNull();
    }

    @Test
    void shouldCreateNumberAttributeWithLong() {
        // Given we have a long attribute
        AttributeValue value = DynamoDBAttributes.createNumberAttribute(123L);
        // When we construct a record with a key for the attribute value
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, value);
        // Then the long attribute should be read from the record
        assertThat(DynamoDBAttributes.getLongAttribute(item, TEST_KEY, 0L)).isEqualTo(123L);
    }

    @Test
    void shouldCreateNumberAttributeWithInt() {
        // Given we have an integer attribute
        AttributeValue value = DynamoDBAttributes.createNumberAttribute(123);
        // When we construct a record with a key for the attribute value
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, value);
        // Then the integer attribute should be read from the record
        assertThat(DynamoDBAttributes.getIntAttribute(item, TEST_KEY, 0)).isEqualTo(123);
    }

    @Test
    void shouldCreateNumberAttributeWithNull() {
        assertThat(DynamoDBAttributes.createNumberAttribute(null)).isNull();
    }

    @Test
    void shouldCreateNumberAttributeWithInstant() {
        // Given we have a long attribute based on an Instant
        Instant time = Instant.parse("2022-11-07T16:00:00.001Z");
        AttributeValue value = DynamoDBAttributes.createNumberAttribute(time.toEpochMilli());
        // When we construct a record with a key for the attribute value
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, value);
        // Then the Instant attribute should be read from the record
        assertThat(DynamoDBAttributes.getInstantAttribute(item, TEST_KEY)).isEqualTo(time);
    }

    @Test
    void shouldGetDoubleAttribute() {
        // Given
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, DynamoDBAttributes.createNumberAttribute(123.456));

        // When/Then
        assertThat(DynamoDBAttributes.getDoubleAttribute(item, TEST_KEY, 0))
                .isEqualTo(123.456);
    }

    @Test
    void shouldGetDoubleAttributeWhenAttributeSetToNull() {
        // Given
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, null);

        // When/Then
        assertThat(DynamoDBAttributes.getDoubleAttribute(item, TEST_KEY, 0))
                .isEqualTo(Double.valueOf(Double.NaN));
    }

    @Test
    void shouldNotGetDoubleAttributeWhenAttributeNotSet() {
        // Given
        Map<String, AttributeValue> item = new HashMap<>();

        // When/Then
        assertThat(DynamoDBAttributes.getDoubleAttribute(item, TEST_KEY, 0))
                .isZero();
    }

    @Test
    void shouldGetStringListAttribute() {
        // Given
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, createListAttribute(List.of(
                createStringAttribute("test1"), createStringAttribute("test2"))));

        // When/Then
        assertThat(DynamoDBAttributes.getStringListAttribute(item, TEST_KEY))
                .isEqualTo(List.of("test1", "test2"));
    }

    @Test
    void shouldNotGetStringListAttributeWithNull() {
        // Given
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TEST_KEY, null);

        // When/Then
        assertThat(DynamoDBAttributes.getStringListAttribute(item, TEST_KEY)).isNull();
    }
}
