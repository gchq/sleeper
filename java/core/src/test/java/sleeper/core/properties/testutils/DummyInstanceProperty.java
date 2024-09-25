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

package sleeper.core.properties.testutils;

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.instance.InstancePropertyGroup;

/**
 * A fake instance property to test working with instance property values.
 */
public class DummyInstanceProperty implements InstanceProperty {
    private final String propertyName;

    public DummyInstanceProperty(String propertyName) {
        this.propertyName = propertyName;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getDefaultValue() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public PropertyGroup getPropertyGroup() {
        return InstancePropertyGroup.COMMON;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return false;
    }
}
