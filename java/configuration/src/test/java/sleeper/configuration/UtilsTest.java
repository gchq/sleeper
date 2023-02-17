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
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.configuration.Utils.combineLists;

public class UtilsTest {

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
        assertThatCode(() -> Utils.isPositiveInteger("ABC"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldNotThrowExceptionDuringPositiveLongCheck() {
        // When/Then
        assertThatCode(() -> Utils.isPositiveLong("ABC"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldNotThrowExceptionDuringPositiveDoubleCheck() {
        // When/Then
        assertThatCode(() -> Utils.isPositiveDouble("ABC"))
                .doesNotThrowAnyException();
    }
}
