/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.clients.admin;

import sleeper.configuration.properties.SystemDefinedInstanceProperty;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TableProperty;

public class SleeperPropertyUtils {

    private SleeperPropertyUtils() {
    }

    public static UserDefinedInstanceProperty getValidInstanceProperty(String propertyName, String propertyValue) {
        if (SystemDefinedInstanceProperty.has(propertyName)) {
            throw new IllegalArgumentException(String.format(
                    "Sleeper property %s is a system-defined property and cannot be updated", propertyName));
        }
        UserDefinedInstanceProperty property = UserDefinedInstanceProperty.getByName(propertyName)
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Sleeper property %s does not exist and cannot be updated", propertyName)));
        if (!property.validationPredicate().test(propertyValue)) {
            throw new IllegalArgumentException(String.format("Sleeper property %s is invalid", propertyName));
        }
        return property;
    }

    public static TableProperty getValidTableProperty(String propertyName, String propertyValue) {
        TableProperty property = TableProperty.from(propertyName)
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "Sleeper property %s does not exist and cannot be updated", propertyName)));
        if (!property.validationPredicate().test(propertyValue)) {
            throw new IllegalArgumentException(String.format("Sleeper property %s is invalid", propertyName));
        }
        return property;
    }
}
