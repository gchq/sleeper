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

import sleeper.core.util.cli.CommandOption.NumArgs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommandArgumentsHelpTest extends CommandArgumentsTestBase {

    @Nested
    @DisplayName("Help summary")
    class HelpSummary {

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

    @Nested
    @DisplayName("Help text per option")
    class HelpPerOption {

        @Test
        void shouldSetHelpTextForLongOption() {
            // Given
            setOptions(CommandOption.withLongName("test").helpText("A test option.").build());

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help, --test

                    --test
                    A test option.""");
        }

        @Test
        void shouldSetHelpTextForShortOption() {
            // Given
            setOptions(CommandOption.withLongName("test").shortName('t').helpText("A test option.").build());

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help, --test

                    --test, -t
                    A test option.""");
        }

        @Test
        void shouldSetHelpTextForMultipleOptions() {
            // Given
            setOptions(
                    CommandOption.withLongName("first").helpText("First option.").build(),
                    CommandOption.withLongName("second").helpText("Second option.").build(),
                    CommandOption.withLongName("third").helpText("Third option.").build());

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help, --first, --second, --third

                    --first
                    First option.

                    --second
                    Second option.

                    --third
                    Third option.""");
        }

        @Test
        void shouldShowHelpSummaryAndOption() {
            // Given
            setHelpSummary("This is a test command.");
            setOptions(
                    CommandOption.withLongName("option").helpText("A test option.").build());

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help, --option

                    This is a test command.

                    --option
                    A test option.""");
        }

        @Test
        void shouldShowMultilineHelpSummaryAndOption() {
            // Given
            setHelpSummary("This is a test command.\n\nIt has some extra help text.");
            setOptions(
                    CommandOption.withLongName("option").helpText("A test option.\nIt has some more information.").build());

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help, --option

                    This is a test command.

                    It has some extra help text.

                    --option
                    A test option.
                    It has some more information.""");
        }

        @Test
        void shouldSetHelpTextForOptionWithArgument() {
            // Given
            setOptions(CommandOption.withLongName("option")
                    .numArgs(NumArgs.ONE)
                    .argsHelpText("<value>")
                    .helpText("A test option.")
                    .build());

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Available options: --help, --option

                    --option <value>
                    A test option.""");
        }

        @Test
        void shouldFailToSetHelpTextWithoutNamedArgument() {
            // Given
            CommandOption.Builder builder = CommandOption.withLongName("option").helpText("A test option.").numArgs(NumArgs.ONE);

            // When / Then
            assertThatThrownBy(builder::build)
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("argsHelpText must be set when helpText is set for an option with arguments");
        }

        @Test
        void shouldFailToSetArgsHelpTextWithoutHelpText() {
            // Given
            CommandOption.Builder builder = CommandOption.withLongName("option").argsHelpText("<value>").numArgs(NumArgs.ONE);

            // When / Then
            assertThatThrownBy(builder::build)
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("helpText must be set when argsHelpText is set");
        }

        @Test
        void shouldFailToSetArgsHelpTextWithoutAllowingAnyArguments() {
            // Given
            CommandOption.Builder builder = CommandOption.withLongName("option").helpText("A test option.").argsHelpText("<value>");

            // When / Then
            assertThatThrownBy(builder::build)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("cannot set argsHelpText for an option taking no arguments");
        }
    }
}
