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
package sleeper.core.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteArrayTest {

    @Test
    void shouldProvideByteArrayAsPrimitiveType() {
        byte[] data = {1, 2, 3};
        // When
        ByteArray result = ByteArray.wrap(data);

        //Then
        assertThat(result.getArray()).isEqualTo(data);
        assertThat(result.getLength()).isEqualTo(data.length);
    }

    @Test
    void shouldCompareByteArraysCorrectly() {
        // Given
        ByteArray first = ByteArray.wrap(new byte[]{7, 8, 9});
        ByteArray second = ByteArray.wrap(new byte[]{7, 8, 9});

        // When / Then
        assertThat(first.getArray()).isEqualTo(second.getArray());
    }

    @Test
    void shouldGetArrayFromObject() {
        // Given
        byte[] data = {'a', 'b', 'c', 'd', 'e'};

        // When
        ByteArray result = ByteArray.wrap(data);

        // Then
        assertThat(result.getArray()).isEqualTo(data);
    }

    @Test
    void shouldValidateEqualsMethodWithNullObjects() {
        // When / Then
        assertThat(ByteArray.equals(null, null)).isTrue();
    }

    @Test
    void shouldValidateEqualsMethodsWithDeclaredEmptyArrays() {
        // Given
        ByteArray first = ByteArray.wrap(null);
        ByteArray second = ByteArray.wrap(null);

        // When / Then
        assertThat(ByteArray.equals(first, second)).isTrue();
    }

    @Test
    void shouldAllowUsageWithinAHashSet() {
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
        assertThat(treeSet).doesNotContain(second);
    }
}
