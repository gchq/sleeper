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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SystemDefinedInstancePropertyImpl implements SystemDefinedInstanceProperty {

    private static final List<SystemDefinedInstanceProperty> ALL = new ArrayList<>();

    private final String propertyName;

    private SystemDefinedInstancePropertyImpl(String propertyName) {
        this.propertyName = propertyName;
    }

    static SystemDefinedInstanceProperty named(String propertyName) {
        return addToAllList(new SystemDefinedInstancePropertyImpl(propertyName));
    }

    static List<SystemDefinedInstanceProperty> all() {
        return Collections.unmodifiableList(ALL);
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getDefaultValue() {
        return null;
    }

    public String toString() {
        return propertyName;
    }

    private static SystemDefinedInstanceProperty addToAllList(SystemDefinedInstanceProperty property) {
        ALL.add(property);
        return property;
    }
}
