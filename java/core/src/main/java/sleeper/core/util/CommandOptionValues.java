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

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Records command line options that were set.
 */
public class CommandOptionValues {
    private final Set<String> optionsSet = new LinkedHashSet<>();

    /**
     * Records that an option was set as a flag.
     *
     * @param option the option
     */
    public void setFlag(CommandOption option) {
        optionsSet.add(option.longName());
        if (option.shortName() != null) {
            optionsSet.add("" + option.shortName());
        }
    }

    /**
     * Checks whether a flag was set.
     *
     * @param  name the name of the flag
     * @return      true if the flag was set
     */
    public boolean isSet(String name) {
        return optionsSet.contains(name);
    }

}
