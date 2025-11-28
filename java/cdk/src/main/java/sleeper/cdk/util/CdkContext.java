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
package sleeper.cdk.util;

import software.constructs.Construct;

import sleeper.core.properties.model.SleeperPropertyValueUtils;

import java.util.List;

public interface CdkContext {

    String tryGetContext(String property);

    default boolean getBooleanOrDefault(String property, boolean defaultValue) {
        String string = tryGetContext(property);
        if (string == null || string.isBlank()) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(string)) {
            return true;
        }
        if ("false".equalsIgnoreCase(string)) {
            return false;
        }
        throw new IllegalArgumentException("Expected true or false for context variable: " + property);
    }

    default List<String> getList(String property) {
        return SleeperPropertyValueUtils.readList(tryGetContext(property));
    }

    static CdkContext from(Construct scope) {
        return key -> (String) scope.getNode().tryGetContext(key);
    }

}
