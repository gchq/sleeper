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

package sleeper.configuration;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.Utils.combineLists;

class UtilsTest {

    @Test
    void shouldCombineLists() {
        // Given
        List<String> list1 = List.of("test1", "test2");
        List<String> list2 = List.of("test3", "test4");

        // When
        List<String> combinedList = combineLists(list1, list2);

        // Then
        assertThat(combinedList)
                .containsExactly("test1", "test2", "test3", "test4");
    }

    @Test
    void shouldNotThrowExceptionDuringPositiveIntegerCheck() {
        // When/Then
        assertThat(Utils.isPositiveInteger("123"))
                .isTrue();
        assertThat(Utils.isPositiveInteger("ABC"))
                .isFalse();
    }

    @Test
    void shouldNotThrowExceptionDuringPositiveLongCheck() {
        // When/Then
        assertThat(Utils.isPositiveLong("123"))
                .isTrue();
        assertThat(Utils.isPositiveLong("ABC"))
                .isFalse();
    }

    @Test
    void shouldNotThrowExceptionDuringPositiveDoubleCheck() {
        // When/Then
        assertThat(Utils.isPositiveDouble("123"))
                .isTrue();
        assertThat(Utils.isPositiveDouble("ABC"))
                .isFalse();
    }

    @Test
    void shouldReadBytesSize() {
        assertThat(Utils.readBytes("42")).isEqualTo(42L);
    }

    @Test
    void shouldReadKBSize() {
        assertThat(Utils.readBytes("2K")).isEqualTo(2048L);
    }

    @Test
    void shouldReadMBSize() {
        assertThat(Utils.readBytes("2M")).isEqualTo(2 * 1024 * 1024L);
    }

    @Test
    void shouldReadGBSize() {
        assertThat(Utils.readBytes("2G")).isEqualTo(2 * 1024 * 1024 * 1024L);
    }
}
