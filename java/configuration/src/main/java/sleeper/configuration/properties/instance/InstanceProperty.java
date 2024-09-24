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

import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.SleeperPropertyIndex;
import sleeper.core.properties.SleeperPropertyValues;

import java.util.List;

/**
 * An instance property definition. This file also contains an index of definitions of all instance properties, which
 * can be accessed via {@link InstanceProperties#getPropertiesIndex}.
 */
public interface InstanceProperty extends SleeperProperty {

    static List<InstanceProperty> getAll() {
        return Index.INSTANCE.getAll();
    }

    /**
     * An index of all instance property definitions.
     */
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

    /**
     * Performs post-processing on the value of this index property. For properties that default to the value of another
     * property, that is retrieved here. Querying for the value of this property will create an infinite loop.
     *
     * @param  value              the value before post-processing
     * @param  instanceProperties the values of all instance properties (do not query for this property)
     * @return                    the value of this property
     */
    default String computeValue(String value, SleeperPropertyValues<InstanceProperty> instanceProperties) {
        return value;
    }
}
