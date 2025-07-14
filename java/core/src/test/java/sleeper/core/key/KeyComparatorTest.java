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
package sleeper.core.key;

import org.junit.jupiter.api.Test;

import sleeper.core.row.KeyComparator;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.StringType;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class KeyComparatorTest {

    @Test
    public void shouldCompareIntKeysCorrectly() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new IntType());

        // When
        int comparison1 = keyComparator.compare(Key.create(1), Key.create(2));
        int comparison2 = keyComparator.compare(Key.create(1), Key.create(1));
        int comparison3 = keyComparator.compare(Key.create(2), Key.create(1));

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
    }

    @Test
    public void shouldCompareIntKeysCorrectlyIncludingNull() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new IntType());

        // When
        Key nullInt = Key.create(null);
        int comparison1 = keyComparator.compare(Key.create(1), nullInt);
        int comparison2 = keyComparator.compare(nullInt, Key.create(1000));
        int comparison3 = keyComparator.compare(nullInt, nullInt);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isGreaterThan(0);
        assertThat(comparison3).isZero();
    }

    @Test
    public void shouldCompareLongKeysCorrectly() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new LongType());

        // When
        int comparison1 = keyComparator.compare(Key.create(1L), Key.create(2L));
        int comparison2 = keyComparator.compare(Key.create(1L), Key.create(1L));
        int comparison3 = keyComparator.compare(Key.create(2L), Key.create(1L));

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
    }

    @Test
    public void shouldCompareLongKeysCorrectlyIncludingNull() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new LongType());

        // When
        Key nullInt = Key.create(null);
        int comparison1 = keyComparator.compare(Key.create(1L), nullInt);
        int comparison2 = keyComparator.compare(nullInt, Key.create(1000L));
        int comparison3 = keyComparator.compare(nullInt, nullInt);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isGreaterThan(0);
        assertThat(comparison3).isZero();
    }

    @Test
    public void shouldCompareStringKeysCorrectly() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new StringType());

        // When
        int comparison1 = keyComparator.compare(Key.create("1"), Key.create("2"));
        int comparison2 = keyComparator.compare(Key.create("1"), Key.create("1"));
        int comparison3 = keyComparator.compare(Key.create("2"), Key.create("1"));

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
    }

    @Test
    public void shouldCompareStringKeysCorrectlyIncludingNull() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new StringType());

        // When
        Key nullString = Key.create(null);
        int comparison1 = keyComparator.compare(Key.create("1"), nullString);
        int comparison2 = keyComparator.compare(nullString, Key.create("1000"));
        int comparison3 = keyComparator.compare(nullString, nullString);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isGreaterThan(0);
        assertThat(comparison3).isZero();
    }

    @Test
    public void shouldCompareByteArrayKeysCorrectly() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new ByteArrayType());

        // When
        int comparison1 = keyComparator.compare(Key.create(new byte[]{1, 2}), Key.create(new byte[]{1, 3, 4}));
        int comparison2 = keyComparator.compare(Key.create(new byte[]{1, 1}), Key.create(new byte[]{1, 1}));
        int comparison3 = keyComparator.compare(Key.create(new byte[]{2, 1}), Key.create(new byte[]{2, 0}));

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isZero();
        assertThat(comparison3).isGreaterThan(0);
    }

    @Test
    public void shouldCompareByteArrayKeysCorrectlyIncludingNull() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new ByteArrayType());

        // When
        Key nullByteArray = Key.create(null);
        int comparison1 = keyComparator.compare(Key.create(new byte[]{1, 2}), nullByteArray);
        int comparison2 = keyComparator.compare(nullByteArray, Key.create(new byte[]{1, 1}));
        int comparison3 = keyComparator.compare(nullByteArray, nullByteArray);

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isGreaterThan(0);
        assertThat(comparison3).isZero();
    }

    @Test
    public void shouldCompare2IntKeysCorrectly() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new IntType(), new IntType());

        // When
        int comparison1 = keyComparator.compare(Key.create(Arrays.asList(1, 1)), Key.create(Arrays.asList(1, 2)));
        int comparison2 = keyComparator.compare(Key.create(Arrays.asList(1, 1)), Key.create(Arrays.asList(2, 2)));
        int comparison3 = keyComparator.compare(Key.create(Arrays.asList(1, 1)), Key.create(Arrays.asList(1, 1)));
        int comparison4 = keyComparator.compare(Key.create(Arrays.asList(2, 1)), Key.create(Arrays.asList(1, 1)));
        int comparison5 = keyComparator.compare(Key.create(Arrays.asList(1, 2)), Key.create(Arrays.asList(1, 1)));

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isLessThan(0);
        assertThat(comparison3).isZero();
        assertThat(comparison4).isGreaterThan(0);
        assertThat(comparison5).isGreaterThan(0);
    }

    @Test
    public void shouldCompare1IntKeyAnd1StringKeyCorrectly() {
        // Given
        KeyComparator keyComparator = new KeyComparator(new IntType(), new StringType());

        // When
        int comparison1 = keyComparator.compare(Key.create(Arrays.asList(1, "1")), Key.create(Arrays.asList(1, "2")));
        int comparison2 = keyComparator.compare(Key.create(Arrays.asList(1, "1")), Key.create(Arrays.asList(2, "2")));
        int comparison3 = keyComparator.compare(Key.create(Arrays.asList(1, "1")), Key.create(Arrays.asList(1, "1")));
        int comparison4 = keyComparator.compare(Key.create(Arrays.asList(2, "1")), Key.create(Arrays.asList(1, "1")));
        int comparison5 = keyComparator.compare(Key.create(Arrays.asList(1, "2")), Key.create(Arrays.asList(1, "1")));

        // Then
        assertThat(comparison1).isLessThan(0);
        assertThat(comparison2).isLessThan(0);
        assertThat(comparison3).isZero();
        assertThat(comparison4).isGreaterThan(0);
        assertThat(comparison5).isGreaterThan(0);
    }
}
