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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommandArgumentsTest {

    @Test
    void shouldReadPositionalArguments() {
        // Given
        CommandArguments arguments = CommandArguments.builder()
                .positionalArguments("first", "second", "third")
                .parse("a", "b", "c");

        // When / Then
        assertThat(arguments.getString("first")).isEqualTo("a");
        assertThat(arguments.getString("second")).isEqualTo("b");
        assertThat(arguments.getString("third")).isEqualTo("c");
    }

    @Test
    void shouldFailWithTooFewArguments() {
        // Given
        CommandArguments.Builder builder = CommandArguments.builder()
                .positionalArguments("first", "second", "third");

        // When / Then
        assertThatThrownBy(() -> builder.parse("a", "b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Usage: <first> <second> <third>");
    }

    @Test
    void shouldFailWithTooManyArguments() {
        // Given
        CommandArguments.Builder builder = CommandArguments.builder()
                .positionalArguments("one", "two");

        // When / Then
        assertThatThrownBy(() -> builder.parse("a", "b", "c"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Usage: <one> <two>");
    }

}
