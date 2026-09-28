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

package sleeper.clients.report;

import sleeper.core.util.cli.CommandArguments;
import sleeper.core.util.cli.CommandArgumentsException;
import sleeper.core.util.cli.CommandOption;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

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
public class ReportTypeArgument<T> {

    public static final String OPTION_NAME = "report-type";

    private final Map<String, T> reporterByType;
    private final String defaultType;

    private ReportTypeArgument(Builder<T> builder) {
        reporterByType = builder.reporterByType;
        defaultType = builder.defaultType;
    }

    /**
     * Creates a builder, starting with the reporter to use when the option is not set.
     *
     * @param  <T>      the type of reporter this resolves to
     * @param  type     the name of the output format, in upper case
     * @param  reporter the reporter
     * @return          the builder
     */
    public static <T> Builder<T> withDefault(String type, T reporter) {
        return new Builder<T>().addDefaultReporter(type, reporter);
    }

    /**
     * Creates the command line option to declare in the usage for a report command.
     *
     * @return the option
     */
    public static CommandOption option() {
        return CommandOption.longOption(OPTION_NAME);
    }

    /**
     * Creates the section of a help summary describing this option. Report commands include this in their help
     * summary, in alphabetical order with their other options.
     *
     * @return the help text
     */
    public String helpText() {
        return "--" + OPTION_NAME + " <type>\n" +
                "Output format. One of " + validTypes() + ". Defaults to " + defaultType + ".";
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
        T reporter = reporterByType.get(setType.toUpperCase(Locale.ROOT));
        if (reporter == null) {
            throw new CommandArgumentsException(
                    "Report type not supported: " + setType + ". Valid types: " + validTypes());
        }
        return reporter;
    }

    private String validTypes() {
        return String.join(", ", reporterByType.keySet());
    }

    /**
     * A builder for this class.
     *
     * @param <T> the type of reporter this resolves to
     */
    public static class Builder<T> {
        private final Map<String, T> reporterByType = new LinkedHashMap<>();
        private String defaultType;

        private Builder() {
        }

        /**
         * Adds a reporter the user may select. Reporters are listed to the user in the order they are added here.
         *
         * @param  type     the name of the output format, in upper case
         * @param  reporter the reporter
         * @return          this builder
         */
        public Builder<T> addReporter(String type, T reporter) {
            reporterByType.put(type, reporter);
            return this;
        }

        public ReportTypeArgument<T> build() {
            return new ReportTypeArgument<>(this);
        }

        private Builder<T> addDefaultReporter(String type, T reporter) {
            defaultType = type;
            return addReporter(type, reporter);
        }
    }
}
