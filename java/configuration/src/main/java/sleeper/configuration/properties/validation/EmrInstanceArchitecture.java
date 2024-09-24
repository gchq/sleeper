/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.configuration.properties.validation;

import org.apache.commons.lang3.EnumUtils;

/**
 * Valid values for AWS EMR instance architecture.
 */
public enum EmrInstanceArchitecture {
    X86_64, ARM64;

    /**
     * Checks if the value is a valid AWS EMR instance architecture.
     *
     * @param  input the value
     * @return       true if it is valid
     */
    public static boolean isValid(String input) {
        if (input == null) {
            return false;
        }
        return SleeperPropertyValueUtils.readList(input).stream()
                .allMatch(architecture -> EnumUtils.isValidEnumIgnoreCase(EmrInstanceArchitecture.class, architecture));
    }
}
