/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.core.util.cli;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommandArgumentsDataTypesTest extends CommandArgumentsTestBase {

    @Nested
    @DisplayName("Read integer argument")
    class ReadInteger {

        @BeforeEach
        void setUp() {
            setOptions(CommandOption.longOption("number"));
        }

        @Test
        void shouldReadPositionalArgument() {
            // Given
            setPositionalArguments("positional");

            // When
            CommandArguments arguments = parse("123");

            // Then
            assertThat(arguments.getInteger("positional")).isEqualTo(123);
        }

        @Test
        void shouldReadOption() {
            assertThat(parse("--number", "123").getInteger("number"))
                    .isEqualTo(123);
        }

        @Test
        void shouldReadDefaultWhenNotSet() {
            assertThat(parse().getIntegerOrDefault("number", 123))
                    .isEqualTo(123);
        }

        @Test
        void shouldReadSetValueWhenDefaulting() {
            assertThat(parse("--number", "123").getIntegerOrDefault("number", 456))
                    .isEqualTo(123);
        }

        @Test
        void shouldFailWhenOptionIsNotSet() {
            // Given
            CommandArguments arguments = parse();

            // When / Then
            assertThatThrownBy(() -> arguments.getInteger("number"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Argument was not set: number");
        }

        @Test
        void shouldFailWhenOptionIsNotANumber() {
            // Given
            CommandArguments arguments = parse("--number", "abc");

            // When / Then
            assertThatThrownBy(() -> arguments.getInteger("number"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected integer for argument \"number\", found \"abc\"");
        }

        @Test
        void shouldFailWhenDefaultingGivenNonNumberValue() {
            // Given
            CommandArguments arguments = parse("--number", "abc");

            // When / Then
            assertThatThrownBy(() -> arguments.getIntegerOrDefault("number", 123))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected integer for argument \"number\", found \"abc\"");
        }
    }

    @Nested
    @DisplayName("Read string argument")
    class ReadString {

        @BeforeEach
        void setUp() {
            setOptions(CommandOption.longOption("string"));
        }

        @Test
        void shouldFailWhenMandatoryArgumentIsNotSet() {
            // Given
            CommandArguments arguments = parse();

            // When / Then
            assertThatThrownBy(() -> arguments.getString("string"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Argument was not set: string");
        }

        @Test
        void shouldFindOptionIsSet() {
            assertThat(parse("--string", "value").getOptionalString("string"))
                    .contains("value");
        }

        @Test
        void shouldFindOptionIsNotSet() {
            assertThat(parse().getOptionalString("string"))
                    .isEmpty();
        }
    }

}
