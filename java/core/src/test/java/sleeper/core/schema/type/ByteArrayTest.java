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
package sleeper.core.schema.type;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteArrayTest {

    @Nested
    @DisplayName("Equality of object")
    class EqualityTest {

        @Test
        void shouldAllowByteArrayToBeReturnedAsPrimitiveType() {
            // Given
            byte[] data = {1, 2, 3};

            // When
            ByteArray result = ByteArray.wrap(data);

            // Then
            assertThat(result.getArray()).isEqualTo(data);
            assertThat(result.getLength()).isEqualTo(data.length);
        }

        @Test
        void shouldMatchWrappedByteArraysUsingSimilarData() {
            // Given
            ByteArray first = ByteArray.wrap(new byte[]{7, 8, 9});
            ByteArray second = ByteArray.wrap(new byte[]{7, 8, 9});

            // When / Then
            assertThat(first).isEqualTo(second);
        }

        @Test
        void shouldMatchWrappedByteArraysUsingSameDataSource() {
            // Given
            byte[] data = {1, 'b', 'c', 4, 5};

            // When
            ByteArray first = ByteArray.wrap(data);
            ByteArray second = ByteArray.wrap(data);

            // Then
            assertThat(first).isEqualTo(second);
        }

        @Test
        void shouldReturnUnequalWhenOneComparatorIsNull() {
            // Given
            ByteArray valid = ByteArray.wrap(new byte[]{'a', 2, 3});

            // When / Then
            assertThat(valid.equals(null)).isFalse();
        }

        @Test
        void shouldReturnEqualsWhenWrappedWithNullData() {
            // Given
            ByteArray first = ByteArray.wrap(null);
            ByteArray second = ByteArray.wrap(null);

            // When / Then
            assertThat(first.equals(second)).isTrue();
        }

        @Test
        void shouldReturnUnequalWhenDataDiffernt() {
            // Given
            ByteArray first = ByteArray.wrap(new byte[]{1, 2, 3});
            ByteArray second = ByteArray.wrap(new byte[]{'a', 'b', 'c'});

            // When / Then
            assertThat(first.equals(second)).isFalse();
        }
    }

    @Nested
    @DisplayName("Sorting contents")
    class NestedTest {
        @Test
        void shouldSortByteArraysWhenContentsNumeric() {
            // Given
            ByteArray first = ByteArray.wrap(new byte[]{1, 2, 3});
            ByteArray second = ByteArray.wrap(new byte[]{4, 5, 6, 7, 8});

            // When / Then
            assertThat(first).isLessThan(second);
        }

        @Test
        void shouldSortByteArraysWhenContentsAreCharacters() {
            // Given
            ByteArray first = ByteArray.wrap(new byte[]{'d', 'e', 'f'});
            ByteArray second = ByteArray.wrap(new byte[]{'a', 'b', 'c'});

            // When / Then
            assertThat(first).isGreaterThan(second);
        }

        @Test
        void shouldSortByteArraysWhenContentsAreMixedPrimitives() {
            ByteArray first = ByteArray.wrap(new byte[]{1, 'b', 3});
            ByteArray second = ByteArray.wrap(new byte[]{'a', 2, 'c', 4});

            assertThat(first).isLessThan(second);
        }

        @Test
        void shouldAllowUsageWithinATreeSet() {
            //Give
            TreeSet<ByteArray> treeSet = new TreeSet<>();
            ByteArray first = ByteArray.wrap(new byte[]{1, 'b', 3});
            ByteArray second = ByteArray.wrap(new byte[]{'a', 2, 'c', 4});

            // When 1
            treeSet.add(first);
            treeSet.add(second);

            // Then 1
            assertThat(treeSet).containsExactlyInAnyOrder(first, second);

            // When 2
            treeSet.remove(second);

            // Then 2
            assertThat(treeSet).containsExactly(first);
        }
    }
}
