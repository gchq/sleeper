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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommandArgumentsTest {

    @Nested
    @DisplayName("Positional arguments")
    class PositionalArguments {

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

    @Nested
    @DisplayName("Long flags")
    class LongFlags {

        @Test
        void shouldReadFlagIsSet() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .options(CommandOption.longFlag("flag"))
                    .parse("--flag");

            // When / Then
            assertThat(arguments.isOptionSet("flag")).isTrue();
        }

        @Test
        void shouldReadFlagIsNotSet() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .options(CommandOption.longFlag("flag"))
                    .parse();

            // When / Then
            assertThat(arguments.isOptionSet("flag")).isFalse();
        }

        @Test
        void shouldReadBeforePositionalArg() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .positionalArguments("positional")
                    .options(CommandOption.longFlag("flag"))
                    .parse("--flag", "value");

            // When / Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isOptionSet("flag")).isTrue();
        }

        @Test
        void shouldReadAfterPositionalArg() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .positionalArguments("positional")
                    .options(CommandOption.longFlag("flag"))
                    .parse("value", "--flag");

            // When / Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isOptionSet("flag")).isTrue();
        }
    }

    @Nested
    @DisplayName("Short flags")
    class ShortFlags {

        @Test
        void shouldReadFlagIsSetShort() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .options(CommandOption.shortFlag('s', "short"))
                    .parse("-s");

            // When / Then
            assertThat(arguments.isOptionSet("s")).isTrue();
            assertThat(arguments.isOptionSet("short")).isTrue();
        }

        @Test
        void shouldReadFlagIsSetLong() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .options(CommandOption.shortFlag('s', "short"))
                    .parse("--short");

            // When / Then
            assertThat(arguments.isOptionSet("s")).isTrue();
            assertThat(arguments.isOptionSet("short")).isTrue();
        }

        @Test
        void shouldReadFlagIsNotSet() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .options(CommandOption.shortFlag('s', "short"))
                    .parse();

            // When / Then
            assertThat(arguments.isOptionSet("s")).isFalse();
            assertThat(arguments.isOptionSet("short")).isFalse();
        }

        @Test
        void shouldReadFlagBeforePositionalArg() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .positionalArguments("positional")
                    .options(CommandOption.shortFlag('s', "short"))
                    .parse("-s", "value");

            // When / Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isOptionSet("short")).isTrue();
        }

        @Test
        void shouldReadFlagAfterPositionalArg() {
            // Given
            CommandArguments arguments = CommandArguments.builder()
                    .positionalArguments("positional")
                    .options(CommandOption.shortFlag('s', "short"))
                    .parse("value", "-s");

            // When / Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isOptionSet("short")).isTrue();
        }
    }

}
