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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A utility to read command line arguments.
 */
public class CommandArguments {

    private final Map<String, String> argByName;
    private final Set<String> optionsSet;

    public CommandArguments(Builder builder) {
        argByName = builder.argByName;
        optionsSet = builder.optionsSet;
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
     * Checks whether a flag was set.
     *
     * @param  name the name of the flag
     * @return      true if the flag was set
     */
    public boolean isFlagSet(String name) {
        return optionsSet.contains(name);
    }

    /**
     * A builder for this class.
     */
    public static class Builder {
        private Map<String, String> argByName = new LinkedHashMap<>();
        private Set<String> optionsSet = new LinkedHashSet<>();

        Builder positionalArg(String name, String value) {
            argByName.put(name, value);
            return this;
        }

        Builder flag(CommandOption option) {
            optionsSet.add(option.longName());
            if (option.shortName() != null) {
                optionsSet.add("" + option.shortName());
            }
            return this;
        }

        public CommandArguments build() {
            return new CommandArguments(this);
        }
    }

}
