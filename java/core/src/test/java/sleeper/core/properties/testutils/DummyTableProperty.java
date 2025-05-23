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
package sleeper.core.properties.testutils;

import sleeper.core.properties.PropertyGroup;
import sleeper.core.properties.SleeperProperty;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.properties.table.TablePropertyComputeValue;
import sleeper.core.properties.table.TablePropertyGroup;

/**
 * A fake table property to test working with table property values.
 */
public class DummyTableProperty implements TableProperty {

    private final TablePropertyComputeValue computeValue;
    private final SleeperProperty defaultProperty;

    private DummyTableProperty(TablePropertyComputeValue computeValue, SleeperProperty defaultProperty) {
        this.computeValue = computeValue;
        this.defaultProperty = defaultProperty;
    }

    /**
     * Creates an instance of this class to read the default value from another property.
     *
     * @param  defaultProperty the default property
     * @return                 the new property
     */
    public static DummyTableProperty defaultedFrom(SleeperProperty defaultProperty) {
        return new DummyTableProperty(TablePropertyComputeValue.defaultProperty(defaultProperty), defaultProperty);
    }

    /**
     * Creates an instance of this class with custom behaviour to post-process the property's values.
     *
     * @param  computeValue the post-processing behaviour
     * @return              the new property
     */
    public static DummyTableProperty customCompute(TablePropertyComputeValue computeValue) {
        return new DummyTableProperty(computeValue, null);
    }

    @Override
    public String getPropertyName() {
        return "made.up";
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
        return TablePropertyGroup.COMPACTION;
    }

    @Override
    public boolean isRunCdkDeployWhenChanged() {
        return false;
    }

    @Override
    public SleeperProperty getDefaultProperty() {
        return defaultProperty;
    }

    @Override
    public String computeValue(String value, InstanceProperties instanceProperties, TableProperties tableProperties) {
        return computeValue.computeValue(value, instanceProperties, tableProperties);
    }
}
