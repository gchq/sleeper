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

package sleeper.clients.report.arguments;

import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;
import sleeper.core.util.cli.CommandOption.NumArgs;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.joining;

/**
 * Reads the output format for a report from the command line. Holds the reporters a report command can output with,
 * and resolves the one the user asked for. Report commands share this so that they declare the same option, accept
 * the same values, and fail the same way.
 * <p>
 * The builder starts with the default reporter, and further reporters are listed to the user in the order they are
 * added after it.
 *
 * @param <T> the type of reporter this resolves to
 */
public class OutputFormatArgument<T> {

    public static final String OPTION_NAME = "format";

    private final Map<String, T> formatToReporter;
    private final String defaultType;

    private OutputFormatArgument(Builder<T> builder) {
        formatToReporter = builder.formatToReporter;
        defaultType = builder.defaultType;
    }

    /**
     * Creates a builder, starting with the reporter to use when the option is not set.
     *
     * @param  <T>      the type of reporter this resolves to
     * @param  format   the name of the output format, in upper case
     * @param  reporter the reporter
     * @return          the builder
     */
    public static <T> Builder<T> withDefault(String format, T reporter) {
        return new Builder<T>().addDefaultReporter(format, reporter);
    }

    /**
     * Creates the command line option to declare in the usage for a report command.
     *
     * @return the option
     */
    public CommandOption option() {
        return CommandOption.withLongName(OPTION_NAME)
                .numArgs(NumArgs.ONE)
                .helpText("Output format. One of " + validFormats() + ". Defaults to " + defaultType + ".")
                .argsHelpText("<format>")
                .build();
    }

    /**
     * Reads the reporter the user asked for. The value is not case sensitive. If the option was not set, the default
     * reporter is returned.
     *
     * @param  arguments the parsed command line arguments
     * @return           the reporter
     */
    public T read(CommandArguments arguments) {
        String setType = arguments.getOptionalString(OPTION_NAME).orElse(defaultType);
        T reporter = formatToReporter.get(setType.toUpperCase(Locale.ROOT));
        if (reporter == null) {
            throw new CommandArgumentsException(
                    "Output format not supported: " + setType + ". Valid formats: " + validFormats());
        }
        return reporter;
    }

    private String validFormats() {
        return formatToReporter.keySet().stream().sorted().collect(joining(", "));
    }

    /**
     * A builder for this class.
     *
     * @param <T> the type of reporter this resolves to
     */
    public static class Builder<T> {
        private final Map<String, T> formatToReporter = new LinkedHashMap<>();
        private String defaultType;

        private Builder() {
        }

        /**
         * Adds a reporter the user may select.
         *
         * @param  format   the name of the output format, in upper case
         * @param  reporter the reporter
         * @return          this builder
         */
        public Builder<T> addReporter(String format, T reporter) {
            formatToReporter.put(format, reporter);
            return this;
        }

        public OutputFormatArgument<T> build() {
            return new OutputFormatArgument<>(this);
        }

        private Builder<T> addDefaultReporter(String format, T reporter) {
            defaultType = format;
            return addReporter(format, reporter);
        }
    }
}
