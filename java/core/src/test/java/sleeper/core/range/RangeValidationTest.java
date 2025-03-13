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
package sleeper.core.range;

import com.facebook.collections.ByteArray;
import org.junit.jupiter.api.Test;

import sleeper.core.schema.Field;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RangeValidationTest {

    @Test
    void shouldNotCreateRangeWithWrongTypeMin() {
        // Given
        Field field = new Field("key", new LongType());
        Object min = 1;
        Object max = 2L;

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Could not read minimum value for field key")
                .cause().isInstanceOf(ClassCastException.class);
    }

    @Test
    void shouldNotCreateRangeWithWrongTypeMax() {
        // Given
        Field field = new Field("key", new LongType());
        Object min = 1L;
        Object max = 2;

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Could not read maximum value for field key")
                .cause().isInstanceOf(ClassCastException.class);
    }

    @Test
    void shouldNotCreateRangeWithNullMin() {
        // Given
        Field field = new Field("key", new StringType());

        // When / Then
        assertThatThrownBy(() -> new Range(field, null, "a"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Minimum value must not be null for field key");
    }

    @Test
    void shouldNotCreateRangeWithWrongTypeByteArray() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Object min = new byte[]{1};
        Object max = ByteArray.wrap(new byte[]{2});

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Could not read maximum value for field key")
                .cause().isInstanceOf(ClassCastException.class);
    }

    @Test
    void shouldNotCreateRangeWithMinLongGreaterThanMax() {
        // Given
        Field field = new Field("key", new LongType());
        Object min = 2L;
        Object max = 1L;

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Range of field key has minimum greater than maximum");
    }

    @Test
    void shouldNotCreateRangeWithMinIntGreaterThanMax() {
        // Given
        Field field = new Field("key", new IntType());
        Object min = 2;
        Object max = 1;

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Range of field key has minimum greater than maximum");
    }

    @Test
    void shouldNotCreateRangeWithMinStringGreaterThanMax() {
        // Given
        Field field = new Field("key", new StringType());
        Object min = "b";
        Object max = "a";

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Range of field key has minimum greater than maximum");
    }

    @Test
    void shouldNotCreateRangeWithMinByteArrayGreaterThanMax() {
        // Given
        Field field = new Field("key", new ByteArrayType());
        Object min = new byte[]{2};
        Object max = new byte[]{1};

        // When / Then
        assertThatThrownBy(() -> new Range(field, min, max))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Range of field key has minimum greater than maximum");
    }
}
