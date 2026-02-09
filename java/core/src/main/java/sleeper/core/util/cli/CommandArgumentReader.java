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
        CommandArgumentReader reader = new CommandArgumentReader(List.of(args));
        ArgumentTracker argumentTracker = new ArgumentTracker();
        while (reader.isArg()) {
            if (!reader.readLongOption(usage, argumentTracker)
                    && !reader.readShortOption(usage, argumentTracker)) {
                argumentTracker.positionalArgument(reader.readPositionalArg());
            }
        }
        return argumentTracker.buildArguments(usage);
    }

    private boolean readLongOption(CommandLineUsage usage, ArgumentTracker tracker) {
        if (!arg().startsWith("--")) {
            return false;
        }
        if (arg().length() < 3) {
            throw new CommandArgumentsException("Incomplete flag option: " + arg());
        }
        String afterDashes = arg().substring(2);
        int equalsPos = afterDashes.indexOf('=');
        if (equalsPos > 0) {
            String name = afterDashes.substring(0, equalsPos);
            String value = afterDashes.substring(equalsPos + 1, afterDashes.length());
            return readOptionWithValue(usage.getLongOption(name), value, usage, tracker);
        }
        Optional<CommandOption> optionOpt = usage.getLongOption(afterDashes);
        if (!optionOpt.isPresent()) {
            return false;
        }
        CommandOption option = optionOpt.get();
        if (option.isFlag()) {
            tracker.flag(option, true);
        } else {
            tracker.option(option, readOptionArg(option));
        }
        advance();
        return true;
    }

    private boolean readOptionWithValue(Optional<CommandOption> optionOpt, String value, CommandLineUsage usage, ArgumentTracker tracker) {
        if (!optionOpt.isPresent()) {
            return false;
        }
        CommandOption option = optionOpt.get();

        if (option.isFlag()) {
            tracker.flag(option, parseBoolean(value));
        } else {
            tracker.option(option, value);
        }
        advance();
        return true;
    }

    private boolean readShortOption(CommandLineUsage usage, ArgumentTracker tracker) {
        if (!arg().startsWith("-")) {
            return false;
        }
        if (arg().length() < 2) {
            throw new CommandArgumentsException("Incomplete flag option: " + arg());
        }
        Optional<CommandOption> optionOpt = usage.getShortOption(arg().charAt(1));
        if (!optionOpt.isPresent()) {
            return false;
        }
        CommandOption option = optionOpt.get();
        if (option.isFlag()) {
            tracker.flag(option, true);
            char[] otherFlags = arg().substring(2).toCharArray();
            for (char flagChar : otherFlags) {
                CommandOption otherOption = usage.getShortOption(flagChar)
                        .filter(CommandOption::isFlag)
                        .orElseThrow(() -> new CommandArgumentsException("Unrecognised flag option: " + flagChar));
                tracker.flag(otherOption, true);
            }
        } else {
            tracker.option(option, readShortOptionArg(option));
        }
        advance();
        return true;
    }

    private String readPositionalArg() {
        String arg = arg();
        advance();
        return arg;
    }

    private String readShortOptionArg(CommandOption option) {
        String combinedArg = arg().substring(2);
        if (!combinedArg.isEmpty()) {
            return combinedArg;
        }
        return readOptionArg(option);
    }

    private String readOptionArg(CommandOption option) {
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

    private boolean parseBoolean(String string) {
        if ("true".equalsIgnoreCase(string)) {
            return true;
        } else if ("false".equalsIgnoreCase(string)) {
            return false;
        } else {
            throw new CommandArgumentsException("Expected boolean, found " + string);
        }
    }

}
