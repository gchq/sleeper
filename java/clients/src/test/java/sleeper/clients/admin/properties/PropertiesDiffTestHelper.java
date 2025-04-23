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
package sleeper.clients.admin.properties;

import sleeper.core.properties.SleeperProperty;

/**
 * A test helper for creating Sleeper configuration property diffs.
 */
public class PropertiesDiffTestHelper {
    private PropertiesDiffTestHelper() {
    }

    /**
     * Creates a diff for a property that changed value.
     *
     * @param  property the property
     * @param  before   the value before the change
     * @param  after    the value after the change
     * @return          the diff
     */
    public static PropertyDiff valueChanged(SleeperProperty property, String before, String after) {
        return valueChanged(property.getPropertyName(), before, after);
    }

    /**
     * Creates a diff for a property whose value was removed from the configuration.
     *
     * @param  property the property
     * @param  value    the value before the change
     * @return          the diff
     */
    public static PropertyDiff valueDeleted(SleeperProperty property, String value) {
        return valueDeleted(property.getPropertyName(), value);
    }

    /**
     * Creates a diff for a property whose value was added to the configuration, and was not set before.
     *
     * @param  property the property
     * @param  value    the value after the change
     * @return          the diff
     */
    public static PropertyDiff newValue(SleeperProperty property, String value) {
        return newValue(property.getPropertyName(), value);
    }

    /**
     * Creates a diff for a property that changed value.
     *
     * @param  property the property name
     * @param  before   the value before the change
     * @param  after    the value after the change
     * @return          the diff
     */
    public static PropertyDiff valueChanged(String property, String before, String after) {
        return new PropertyDiff(property, before, after);
    }

    /**
     * Creates a diff for a property whose value was removed from the configuration.
     *
     * @param  property the property name
     * @param  value    the value before the change
     * @return          the diff
     */
    public static PropertyDiff valueDeleted(String property, String value) {
        return new PropertyDiff(property, value, null);
    }

    /**
     * Creates a diff for a property whose value was added to the configuration, and was not set before.
     *
     * @param  property the property name
     * @param  value    the value after the change
     * @return          the diff
     */
    public static PropertyDiff newValue(String property, String value) {
        return new PropertyDiff(property, null, value);
    }
}
