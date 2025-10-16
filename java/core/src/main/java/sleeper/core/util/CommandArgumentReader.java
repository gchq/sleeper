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
import java.util.Optional;

/**
 * A utility for parsing command line arguments.
 */
public class CommandArgumentReader {

    private final List<String> arguments;
    private int index;

    public CommandArgumentReader(List<String> arguments) {
        this.arguments = arguments;
    }

    public static CommandArguments parse(CommandLineUsage usage, String... args) {
        CommandArguments.Builder builder = CommandArguments.builder();
        CommandArgumentReader reader = new CommandArgumentReader(List.of(args));
        List<String> positionalValues = new ArrayList<>();
        while (reader.isArg()) {
            if (reader.readOption(usage, builder)) {
                continue;
            }
            positionalValues.add(reader.readPositionalArg());
        }
        if (positionalValues.size() != usage.getNumPositionalArgs()) {
            throw usage.usageException();
        }
        for (int i = 0; i < positionalValues.size(); i++) {
            builder.positionalArg(usage.getPositionalArgName(i), positionalValues.get(i));
        }
        return builder.build();
    }

    private boolean readOption(CommandLineUsage usage, CommandArguments.Builder builder) {
        String arg = arg();
        if (arg.startsWith("--")) {
            String longOption = arg.substring(2);
            Optional<CommandOption> option = usage.getLongOption(longOption);
            if (option.isPresent()) {
                builder.flag(option.get());
                advance();
                return true;
            }
        } else if (arg.startsWith("-")) {
            char shortOption = arg.charAt(1);
            Optional<CommandOption> option = usage.getShortOption(shortOption);
            if (option.isPresent()) {
                builder.flag(option.get());
                advance();
                return true;
            }
        }
        return false;
    }

    private String readPositionalArg() {
        String arg = arg();
        advance();
        return arg;
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
