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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * A model of arguments and options for the command line.
 */
public class CommandLineUsage {

    private final List<String> positionalArguments;
    private final Map<String, CommandOption> optionByLongName;
    private final Map<Character, CommandOption> optionByShortName;

    private CommandLineUsage(List<String> positionalArguments, List<CommandOption> options) {
        this.positionalArguments = positionalArguments;
        optionByLongName = options.stream().collect(toMap(CommandOption::longName, Function.identity()));
        optionByShortName = options.stream().filter(option -> option.shortName() != null).collect(toMap(CommandOption::shortName, Function.identity()));
    }

    /**
     * Creates a model of command line usage.
     *
     * @param  positionalArguments the positional arguments
     * @param  options             the options
     * @return                     the model
     */
    public static CommandLineUsage positionalAndOptions(List<String> positionalArguments, List<CommandOption> options) {
        return new CommandLineUsage(positionalArguments, options);
    }

    /**
     * Parses the given command line arguments.
     *
     * @param  args the arguments
     * @return      the parsed arguments
     */
    public CommandArguments parse(String... args) {
        CommandArguments.Builder builder = CommandArguments.builder();
        List<String> positionalValues = new ArrayList<>();
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String longOption = arg.substring(2);
                CommandOption option = optionByLongName.get(longOption);
                if (option != null) {
                    builder.flag(option);
                    continue;
                }
            } else if (arg.startsWith("-")) {
                char shortOption = arg.charAt(1);
                CommandOption option = optionByShortName.get(shortOption);
                if (option != null) {
                    builder.flag(option);
                    continue;
                }
            }
            positionalValues.add(arg);
        }
        if (positionalValues.size() != positionalArguments.size()) {
            throw usageException();
        }
        for (int i = 0; i < positionalValues.size(); i++) {
            builder.positionalArg(positionalArguments.get(i), positionalValues.get(i));
        }
        return builder.build();
    }

    private IllegalArgumentException usageException() {
        return new IllegalArgumentException("Usage: " +
                positionalArguments.stream()
                        .map(name -> "<" + name + ">")
                        .collect(joining(" ")));
    }

}
