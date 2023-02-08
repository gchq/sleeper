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
package sleeper.configuration.properties;

import sleeper.configuration.properties.group.InstancePropertyGroup;
import sleeper.configuration.properties.group.PropertyGroup;

import java.util.Comparator;
import java.util.List;

import static sleeper.configuration.Utils.combineLists;

public interface InstanceProperty extends SleeperProperty {
    static List<InstanceProperty> getAllGroupedProperties() {
        return sortProperties(getAllProperties());
    }

    static List<InstanceProperty> sortProperties(List<InstanceProperty> properties) {
        properties.sort(Comparator.comparingInt(p -> InstancePropertyGroup.all().indexOf(p.getPropertyGroup())));
        return properties;
    }

    static List<InstanceProperty> getAllProperties() {
        return combineLists(List.of(UserDefinedInstanceProperty.values()), List.of(SystemDefinedInstanceProperty.values()));
    }

    PropertyGroup getPropertyGroup();
}
