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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tracks arguments, and the order they were declared. Used in {@link CommandArgumentReader#parse()} to build
 * a {@link CommandArguments} object. As the reader parses the different types of argument, they are provided to this
 * tracker in order, and then they are validated and combined into a {@link CommandArguments} object.
 */
class ArgumentTracker {
    private final List<PositionalArgumentValue> positionalArguments = new ArrayList<>();
    private final Map<String, String> argByName = new LinkedHashMap<>();
    private final Map<String, Boolean> flagByName = new LinkedHashMap<>();
    private int firstPositionalArgumentWithNoOptionsAfter = 0;

    /**
     * Adds a positional argument. If we allow passing through arguments that aren't in the usage for this command,
     * they will be included here as well.
     *
     * @param usage the command line usage
     * @param value the value
     */
    void positionalArgument(CommandLineUsage usage, String value) {
        int index = positionalArguments.size();
        PositionalArgument argument = usage.getPositionalArgument(index).orElse(null);
        positionalArguments.add(new PositionalArgumentValue(value, index, argument));
    }

    /**
     * Sets an option with an argument.
     *
     * @param option   the option that was set
     * @param argument the value of the argument
     */
    void option(CommandOption option, String argument) {
        argByName.put(option.longName(), argument);
        if (option.shortName() != null) {
            argByName.put(Character.toString(option.shortName()), argument);
        }
        firstPositionalArgumentWithNoOptionsAfter = positionalArguments.size();
    }

    /**
     * Sets a flag option.
     *
     * @param option the option that was set as a flag
     * @param isSet  true if the flag should be set
     */
    void flag(CommandOption option, boolean isSet) {
        flagByName.put(option.longName(), isSet);
        if (option.shortName() != null) {
            flagByName.put(Character.toString(option.shortName()), isSet);
        }
        firstPositionalArgumentWithNoOptionsAfter = positionalArguments.size();
    }

    /**
     * Builds a CommandArguments object from the specified arguments.
     *
     * @param  usage the command line usage
     * @return       the arguments object
     */
    CommandArguments buildArguments(CommandLineUsage usage) {
        addPositionalArgumentsToArgByName(usage);
        validateNumberOfPositionalArguments(usage);
        return CommandArguments.builder()
                .argByName(argByName)
                .flagByName(flagByName)
                .passThroughArguments(getPassThroughArguments(usage))
                .build();
    }

    private void addPositionalArgumentsToArgByName(CommandLineUsage usage) {
        for (PositionalArgumentValue value : positionalArguments) {
            value.nameIfRecognised()
                    .ifPresent(name -> argByName.put(name, value.value()));
        }
    }

    private void validateNumberOfPositionalArguments(CommandLineUsage usage) {
        // When displaying the help text, any positional arguments are ignored.
        if (flagByName.getOrDefault("help", false)) {
            return;
        }
        if (usage.isPassThroughExtraArguments()) {
            validatePositionalArgumentsWithPassThrough(usage);
        } else {
            validatePositionalArgumentsWithNoPassThrough(usage);
        }
    }

    private void validatePositionalArgumentsWithPassThrough(CommandLineUsage usage) {
        int expectedUserPositionals = usage.countPositionalArgsSetByUser();
        List<String> foundUserPositionals = getPositionalsSetByUser();
        if (foundUserPositionals.size() < expectedUserPositionals) {
            throw new WrongNumberOfArgumentsException(foundUserPositionals, expectedUserPositionals);
        }
        if (positionalArguments.size() < usage.getNumPositionalArgs()) {
            throw new MissingSystemArgumentException();
        }
        if (firstPositionalArgumentWithNoOptionsAfter > usage.getNumPositionalArgs()) {
            throw new WrongNumberOfArgumentsException(
                    getNonPassthroughPositionalsSetByUser(),
                    expectedUserPositionals);
        }
    }

    private void validatePositionalArgumentsWithNoPassThrough(CommandLineUsage usage) {
        int expectedUserPositionals = usage.countPositionalArgsSetByUser();
        List<String> foundUserPositionals = getPositionalsSetByUser();
        if (foundUserPositionals.size() != expectedUserPositionals) {
            throw new WrongNumberOfArgumentsException(foundUserPositionals, expectedUserPositionals);
        }
        if (positionalArguments.size() != usage.getNumPositionalArgs()) {
            throw new MissingSystemArgumentException();
        }
    }

    private List<String> getPositionalsSetByUser() {
        return positionalArguments.stream()
                .filter(PositionalArgumentValue::setByUser)
                .map(PositionalArgumentValue::value)
                .toList();
    }

    private List<String> getNonPassthroughPositionalsSetByUser() {
        return positionalArguments.stream()
                .limit(firstPositionalArgumentWithNoOptionsAfter)
                .filter(PositionalArgumentValue::setByUser)
                .map(PositionalArgumentValue::value)
                .toList();
    }

    private List<String> getPassThroughArguments(CommandLineUsage usage) {
        return positionalArguments.stream()
                .skip(usage.getNumPositionalArgs())
                .map(PositionalArgumentValue::value)
                .toList();
    }

    /**
     * A value for a positional argument.
     *
     * @param value    the value
     * @param index    the index the value was given at
     * @param argument the argument, or null if there is no positional argument with this index
     */
    private record PositionalArgumentValue(String value, int index, PositionalArgument argument) {

        Optional<String> nameIfRecognised() {
            return Optional.ofNullable(argument)
                    .map(PositionalArgument::name);
        }

        boolean setByUser() {
            return Optional.ofNullable(argument)
                    .map(PositionalArgument::setByUser)
                    .orElse(true);
        }
    }

}
