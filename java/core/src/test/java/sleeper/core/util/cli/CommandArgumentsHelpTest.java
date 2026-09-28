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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandArgumentsHelpTest extends CommandArgumentsTestBase {

    @Nested
    @DisplayName("Help text")
    class HelpText {

        @Test
        void shouldShowBasicUsage() {
            // Given
            setPositionalArguments("a", "b", "c");

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Usage: <a> <b> <c>
                    Available options: --help""");
        }

        @Test
        void shouldAddHelpSummary() {
            // Given
            setHelpSummary("This command does something useful.");
            setPositionalArguments("parameter");

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Usage: <parameter>
                    Available options: --help

                    This command does something useful.""");
        }

        @Test
        void shouldDisplayHelpSummaryWhenNoPositionalArgumentsAreSet() {
            // Given
            setHelpSummary("This command does something useful.");

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help

                    This command does something useful.""");
        }

        @Test
        void shouldFindHelpFlagIsSetWhenNoPositionalParametersAreGiven() {
            // Given
            setPositionalArguments("first", "second");

            // When
            CommandArguments arguments = parse("--help");

            // Then
            assertThat(arguments.isFlagSet("help")).isTrue();
        }
    }

    @Nested
    @DisplayName("Usage message")
    class UsageMessage {

        @Test
        void shouldDisplayPositionalParameters() {
            // Given
            setPositionalArguments("first thing", "next", "last one");

            // When / Then
            assertThat(usageMessage()).isEqualTo("""
                    Usage: <first thing> <next> <last one>
                    Available options: --help""");
        }

        @Test
        void shouldDisplayAvailableOptions() {
            // Given
            setOptions(CommandOption.longFlag("test"), CommandOption.shortOption('o', "other"));

            // When / Then
            assertThat(usageMessage()).isEqualTo("""
                    Available options: --help, --test, --other""");
        }
    }
}
