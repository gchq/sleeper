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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;

/**
 * A utility to read command line arguments.
 */
public class CommandArguments {

    private final Map<String, String> argByName;

    public CommandArguments(Map<String, String> argByName) {
        this.argByName = argByName;
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
     * A builder for this class.
     */
    public static class Builder {
        private List<String> positionalArguments;

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
         * Parses the given command line arguments.
         *
         * @param  args the arguments
         * @return      the parsed arguments
         */
        public CommandArguments parse(String... args) {
            if (args.length != positionalArguments.size()) {
                throw new IllegalArgumentException("Usage: " +
                        positionalArguments.stream()
                                .map(name -> "<" + name + ">")
                                .collect(joining(" ")));
            }
            Map<String, String> argByName = new LinkedHashMap<>();
            for (int i = 0; i < args.length; i++) {
                argByName.put(positionalArguments.get(i), args[i]);
            }
            return new CommandArguments(argByName);
        }
    }

}
