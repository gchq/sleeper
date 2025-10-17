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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A utility for parsing command line arguments.
 */
public class CommandArgumentReader {

    private final List<String> arguments;
    private int index;

    private CommandArgumentReader(List<String> arguments) {
        this.arguments = arguments;
    }

    /**
     * Reads command line arguments and detects arguments and options.
     *
     * @param  usage information about how arguments and options are specified
     * @param  args  the arguments from the command line
     * @return       the parsed arguments
     */
    public static CommandArguments parse(CommandLineUsage usage, String... args) {
        CommandArguments.Builder builder = CommandArguments.builder();
        CommandArgumentReader reader = new CommandArgumentReader(List.of(args));
        List<String> positionalValues = new ArrayList<>();
        while (reader.isArg()) {
            if (!reader.readOption(usage, builder)) {
                positionalValues.add(reader.readPositionalArg());
            }
        }
        if (positionalValues.size() != usage.getNumPositionalArgs()) {
            throw new WrongNumberOfArgumentsException(positionalValues.size(), usage.getNumPositionalArgs());
        }
        for (int i = 0; i < positionalValues.size(); i++) {
            builder.argument(usage.getPositionalArgName(i), positionalValues.get(i));
        }
        return builder.build();
    }

    private boolean readOption(CommandLineUsage usage, CommandArguments.Builder builder) {
        if (readLongOption(usage, builder)) {
            return true;
        } else if (readShortOption(usage, builder)) {
            return true;
        }
        return false;
    }

    private boolean readLongOption(CommandLineUsage usage, CommandArguments.Builder builder) {
        if (!arg().startsWith("--")) {
            return false;
        }
        Optional<CommandOption> option = usage.getLongOption(arg().substring(2));
        if (!option.isPresent()) {
            return false;
        }
        readOption(option.get(), false, builder);
        return true;
    }

    private boolean readShortOption(CommandLineUsage usage, CommandArguments.Builder builder) {
        if (!arg().startsWith("-")) {
            return false;
        }
        Optional<CommandOption> option = usage.getShortOption(arg().charAt(1));
        if (!option.isPresent()) {
            return false;
        }
        readOption(option.get(), true, builder);
        return true;
    }

    private void readOption(CommandOption option, boolean setAsShort, CommandArguments.Builder builder) {
        switch (option.numArgs()) {
            case NONE:
                builder.flag(option);
                break;
            case ONE:
                builder.option(option, readOptionArg(option, setAsShort));
                break;
            default:
                throw new IllegalArgumentException("Unrecognised number of arguments for option: " + option);
        }
        advance();
    }

    private String readPositionalArg() {
        String arg = arg();
        advance();
        return arg;
    }

    private String readOptionArg(CommandOption option, boolean setAsShort) {
        if (setAsShort) {
            try {
                String arg = arg().substring(2);
                Integer.parseInt(arg);
                return arg;
            } catch (NumberFormatException e) {
            }
        }
        advance();
        if (!isArg()) {
            throw new CommandArgumentsException("Expected an argument for option: " + option.longName());
        }
        return arg();
    }

    private boolean isArg() {
        return index < arguments.size();
    }

    private String arg() {
        return arguments.get(index);
    }

    private void advance() {
        index++;
    }

}
