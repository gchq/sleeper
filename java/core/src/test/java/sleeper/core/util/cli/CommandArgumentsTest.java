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
package sleeper.core.util.cli;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import sleeper.core.util.cli.CommandOption.NumArgs;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommandArgumentsTest {

    List<String> positionalArguments = List.of();
    List<CommandOption> options = List.of();
    String helpSummary;

    @Nested
    @DisplayName("Positional arguments")
    class PositionalArguments {

        @Test
        void shouldReadPositionalArguments() {
            // Given
            setPositionalArguments("first", "second", "third");

            // When
            CommandArguments arguments = parse("a", "b", "c");

            // Then
            assertThat(arguments.getString("first")).isEqualTo("a");
            assertThat(arguments.getString("second")).isEqualTo("b");
            assertThat(arguments.getString("third")).isEqualTo("c");
        }

        @Test
        void shouldFailWithTooFewArguments() {
            // Given
            setPositionalArguments("first", "second", "third");

            // When / Then
            assertThatThrownBy(() -> parse("a", "b"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 3 positional arguments, found 2");
        }

        @Test
        void shouldFailWithTooManyArguments() {
            // Given
            setPositionalArguments("one", "two");

            // When / Then
            assertThatThrownBy(() -> parse("a", "b", "c"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 2 positional arguments, found 3");
        }

        @Test
        void shouldFailExpectingOneArgument() {
            // Given
            setPositionalArguments("argument");

            // When / Then
            assertThatThrownBy(() -> parse("a", "b", "c"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected 1 positional argument, found 3");
        }
    }

    @Nested
    @DisplayName("Long flags")
    class LongFlags {

        @BeforeEach
        void setUp() {
            setOptions(CommandOption.longFlag("flag"));
        }

        @Test
        void shouldReadFlagIsSet() {
            assertThat(parse("--flag").isFlagSet("flag")).isTrue();
        }

        @Test
        void shouldReadFlagIsNotSet() {
            assertThat(parse().isFlagSet("flag")).isFalse();
        }

        @Test
        void shouldReadBeforePositionalArg() {
            // Given
            setPositionalArguments("positional");

            // When
            CommandArguments arguments = parse("--flag", "value");

            // Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isFlagSet("flag")).isTrue();
        }

        @Test
        void shouldReadAfterPositionalArg() {
            // Given
            setPositionalArguments("positional");

            // When
            CommandArguments arguments = parse("value", "--flag");

            // Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isFlagSet("flag")).isTrue();
        }

        @Test
        void shouldFailWithIncompleteFlagArgument() {
            assertThatThrownBy(() -> parse("--"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Incomplete flag option: --");
        }
    }

    @Nested
    @DisplayName("Short flags")
    class ShortFlags {

        @BeforeEach
        void setUp() {
            setOptions(CommandOption.shortFlag('s', "short"));
        }

        @Test
        void shouldReadFlagIsSetShort() {
            // When
            CommandArguments arguments = parse("-s");

            // Then
            assertThat(arguments.isFlagSet("s")).isTrue();
            assertThat(arguments.isFlagSet("short")).isTrue();
        }

        @Test
        void shouldReadFlagIsSetLong() {
            // When
            CommandArguments arguments = parse("--short");

            // Then
            assertThat(arguments.isFlagSet("s")).isTrue();
            assertThat(arguments.isFlagSet("short")).isTrue();
        }

        @Test
        void shouldReadFlagIsNotSet() {
            // When
            CommandArguments arguments = parse();

            // Then
            assertThat(arguments.isFlagSet("s")).isFalse();
            assertThat(arguments.isFlagSet("short")).isFalse();
        }

        @Test
        void shouldReadFlagBeforePositionalArg() {
            // Given
            setPositionalArguments("positional");

            // When
            CommandArguments arguments = parse("-s", "value");

            // Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isFlagSet("short")).isTrue();
        }

        @Test
        void shouldReadFlagAfterPositionalArg() {
            // Given
            setPositionalArguments("positional");

            // When
            CommandArguments arguments = parse("value", "-s");

            // Then
            assertThat(arguments.getString("positional")).isEqualTo("value");
            assertThat(arguments.isFlagSet("short")).isTrue();
        }

        @Test
        void shouldReadCombinedFlags() {
            // Given
            setOptions(
                    CommandOption.shortFlag('a', "A"),
                    CommandOption.shortFlag('b', "B"),
                    CommandOption.shortFlag('c', "C"),
                    CommandOption.shortFlag('d', "D"));

            // When
            CommandArguments arguments = parse("-abc");

            // Then
            assertThat(arguments.isFlagSet("a")).isTrue();
            assertThat(arguments.isFlagSet("b")).isTrue();
            assertThat(arguments.isFlagSet("c")).isTrue();
            assertThat(arguments.isFlagSet("d")).isFalse();
        }

        @Test
        void shouldFailWhenCombinedFlagIsUnrecognised() {
            assertThatThrownBy(() -> parse("-sa"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Unrecognised flag option: a");
        }

        @Test
        void shouldFailWithIncompleteFlagArgument() {
            assertThatThrownBy(() -> parse("-"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Incomplete flag option: -");
        }
    }

    @Nested
    @DisplayName("Option with arguments")
    class OptionWithArgs {

        @Test
        void shouldReadLongOptionWithOneArgument() {
            // Given
            setOptions(CommandOption.longOption("option", NumArgs.ONE));

            // When
            CommandArguments arguments = parse("--option", "value");

            // Then
            assertThat(arguments.getString("option")).isEqualTo("value");
        }

        @Test
        void shouldReadShortOptionWithOneArgument() {
            // Given
            setOptions(CommandOption.shortOption('o', "option", NumArgs.ONE));

            // When
            CommandArguments arguments = parse("-o", "value");

            // Then
            assertThat(arguments.getString("option")).isEqualTo("value");
            assertThat(arguments.getString("o")).isEqualTo("value");
        }

        @Test
        void shouldReadShortOptionWithOneArgumentCombined() {
            // Given
            setOptions(CommandOption.shortOption('D', "property", NumArgs.ONE));

            // When
            CommandArguments arguments = parse("-D123");

            // Then
            assertThat(arguments.getString("property")).isEqualTo("123");
            assertThat(arguments.getString("D")).isEqualTo("123");
        }

        @Test
        void shouldFailWhenOptionIsMissingRequiredArgument() {
            // Given
            setOptions(CommandOption.longOption("option", NumArgs.ONE));

            // When / Then
            assertThatThrownBy(() -> parse("--option"))
                    .isInstanceOf(CommandArgumentsException.class)
                    .hasMessage("Expected an argument for option: option");
        }
    }

    @Nested
    @DisplayName("Help text")
    class HelpText {

        @Test
        void shouldShowBasicUsage() {
            // Given
            setPositionalArguments("a", "b", "c");

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Usage: <a> <b> <c>""");
        }

        @Test
        void shouldAddHelpSummary() {
            // Given
            setHelpSummary("This command does something useful.");
            setPositionalArguments("parameter");

            // When / Then
            assertThat(helpText()).isEqualTo("""
                    Usage: <parameter>

                    This command does something useful.""");
        }
    }

    @Nested
    @DisplayName("Read integer argument")
    class ReadInteger {

        @BeforeEach
        void setUp() {
            setOptions(CommandOption.longOption("number", NumArgs.ONE));
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
            setOptions(CommandOption.longOption("string", NumArgs.ONE));
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

    private void setPositionalArguments(String... names) {
        positionalArguments = List.of(names);
    }

    private void setOptions(CommandOption... options) {
        this.options = List.of(options);
    }

    private void setHelpSummary(String helpSummary) {
        this.helpSummary = helpSummary;
    }

    private CommandArguments parse(String... args) {
        return CommandArgumentReader.parse(usage(), args);
    }

    private String helpText() {
        return usage().createHelpText();
    }

    private CommandLineUsage usage() {
        return CommandLineUsage.builder()
                .positionalArguments(positionalArguments)
                .options(options)
                .helpSummary(helpSummary)
                .build();
    }
}
