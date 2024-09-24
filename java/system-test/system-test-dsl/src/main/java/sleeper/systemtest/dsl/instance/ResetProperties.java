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

package sleeper.systemtest.dsl.instance;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.core.properties.SleeperProperty;

import static java.util.function.Predicate.not;

public class ResetProperties {

    private ResetProperties() {
    }

    public static <T extends SleeperProperty> void reset(
            SleeperProperties<T> properties,
            SleeperProperties<T> resetProperties) {
        for (T property : propertiesToReset(properties)) {
            if (resetProperties.isSet(property)) {
                properties.set(property, resetProperties.get(property));
            } else {
                properties.unset(property);
            }
        }
    }

    private static <T extends SleeperProperty> Iterable<T> propertiesToReset(SleeperProperties<T> properties) {
        return () -> properties.getPropertiesIndex().getUserDefined().stream()
                .filter(SleeperProperty::isEditable)
                .filter(not(SleeperProperty::isRunCdkDeployWhenChanged))
                .iterator();
    }
}
