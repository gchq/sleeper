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
package sleeper.configuration.properties.instance;

import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.SleeperPropertyIndex;

import java.util.List;
import java.util.Optional;

public interface InstanceProperty extends SleeperProperty {

    static List<InstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    static boolean has(String propertyName) {
        return Index.INSTANCE.getByName(propertyName).isPresent();
    }

    static Optional<InstanceProperty> getByName(String propertyName) {
        return Index.INSTANCE.getByName(propertyName);
    }

    class Index {
        private Index() {
        }

        static final SleeperPropertyIndex<InstanceProperty> INSTANCE = createInstance();

        private static SleeperPropertyIndex<InstanceProperty> createInstance() {
            SleeperPropertyIndex<InstanceProperty> index = new SleeperPropertyIndex<>();
            index.addAll(UserDefinedInstanceProperty.getAll());
            index.addAll(CdkDefinedInstanceProperty.getAll());
            return index;
        }
    }

    default String computeValue(String value, InstanceProperties instanceProperties) {
        return value;
    }
}
