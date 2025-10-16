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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * A utility to read command line arguments.
 */
public class CommandArguments {

    private final Map<String, String> argByName;
    private final CommandOptionValues optionValues;

    public CommandArguments(Map<String, String> argByName, CommandOptionValues optionValues) {
        this.argByName = argByName;
        this.optionValues = optionValues;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Retrieves the value of a positional argument.
     *
     * @param  name the name of the argument
     * @return      the value
     */
    public String getString(String name) {
        return argByName.get(name);
    }

    /**
     * Retrieves the options that were set.
     *
     * @return the options
     */
    public CommandOptionValues options() {
        return optionValues;
    }

    /**
     * A builder for this class.
     */
    public static class Builder {
        private List<String> positionalArguments = List.of();
        private Map<String, CommandOption> optionByLongName = Map.of();
        private Map<Character, CommandOption> optionByShortName = Map.of();

        /**
         * Sets the names of positional arguments.
         *
         * @param  names the names
         * @return       this builder
         */
        public Builder positionalArguments(String... names) {
            positionalArguments = List.of(names);
            return this;
        }

        /**
         * Sets the options that can be set in addition to positional arguments.
         *
         * @param  options the options
         * @return         this builder
         */
        public Builder options(CommandOption... options) {
            optionByLongName = Stream.of(options).collect(toMap(CommandOption::longName, Function.identity()));
            optionByShortName = Stream.of(options).filter(option -> option.shortName() != null).collect(toMap(CommandOption::shortName, Function.identity()));
            return this;
        }

        /**
         * Parses the given command line arguments.
         *
         * @param  args the arguments
         * @return      the parsed arguments
         */
        public CommandArguments parse(String... args) {
            Map<String, String> argByName = new LinkedHashMap<>();
            List<String> positionalValues = new ArrayList<>();
            CommandOptionValues optionValues = new CommandOptionValues();
            for (String arg : args) {
                if (arg.startsWith("--")) {
                    String longOption = arg.substring(2);
                    CommandOption option = optionByLongName.get(longOption);
                    if (option != null) {
                        optionValues.setFlag(option);
                        continue;
                    }
                } else if (arg.startsWith("-")) {
                    char shortOption = arg.charAt(1);
                    CommandOption option = optionByShortName.get(shortOption);
                    if (option != null) {
                        optionValues.setFlag(option);
                        continue;
                    }
                }
                positionalValues.add(arg);
            }
            if (positionalValues.size() != positionalArguments.size()) {
                throw usageException();
            }
            for (int i = 0; i < positionalValues.size(); i++) {
                argByName.put(positionalArguments.get(i), positionalValues.get(i));
            }
            return new CommandArguments(argByName, optionValues);
        }

        private IllegalArgumentException usageException() {
            return new IllegalArgumentException("Usage: " +
                    positionalArguments.stream()
                            .map(name -> "<" + name + ">")
                            .collect(joining(" ")));
        }
    }

}
