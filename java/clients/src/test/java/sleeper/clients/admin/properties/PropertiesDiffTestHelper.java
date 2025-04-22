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
 * This class is a helper class for the PropertiesDiffTest provided moths to easily generate a PropertyDiff.
 */
public class PropertiesDiffTestHelper {
    private PropertiesDiffTestHelper() {
    }

    /**
     * Returns a new PropertyDiff with the input values.
     *
     * @param  property The SleeperProperty to get the property name from.
     * @param  before   Value to be assigned to the before value of the property.
     * @param  after    Value to be assigned to the after value of the property.
     * @return          A new PropertyDiff with the input values.
     */
    public static PropertyDiff valueChanged(SleeperProperty property, String before, String after) {
        return valueChanged(property.getPropertyName(), before, after);
    }

    /**
     * Returns a new PropertyDiff with no newValue and oldValue set to the input value.
     *
     * @param  property The SleeperProperty to get the property name from.
     * @param  value    The old value to be set.
     * @return          A PropertyDiff with the inputted values and no newValue.
     */
    public static PropertyDiff valueDeleted(SleeperProperty property, String value) {
        return valueDeleted(property.getPropertyName(), value);
    }

    /**
     * Returns a new PropertyDiff with no oldValue and newValue set to the input value.
     *
     * @param  property The SleeperProperty to get the property name from.
     * @param  value    The new value to be set.
     * @return          A PropertyDiff with the inputted values and no oldValue.
     */
    public static PropertyDiff newValue(SleeperProperty property, String value) {
        return newValue(property.getPropertyName(), value);
    }

    /**
     * Returns a new PropertyDiff with the input values.
     *
     * @param  property The String of the propertyName.
     * @param  before   Value to be assigned to the before value of the property.
     * @param  after    Value to be assigned to the after value of the property.
     * @return          A new PropertyDiff with the input values.
     */
    public static PropertyDiff valueChanged(String property, String before, String after) {
        return new PropertyDiff(property, before, after);
    }

    /**
     * Returns a new PropertyDiff with no newValue and oldValue set to the input value.
     *
     * @param  property The String of the property name.
     * @param  value    The old value to be set.
     * @return          A PropertyDiff with the inputted values and no newValue.
     */
    public static PropertyDiff valueDeleted(String property, String value) {
        return new PropertyDiff(property, value, null);
    }

    /**
     * Returns a new PropertyDiff with no oldValue and newValue set to the input value.
     *
     * @param  property The String of the property name.
     * @param  value    The new value to be set.
     * @return          A PropertyDiff with the inputted values and no oldValue.
     */
    public static PropertyDiff newValue(String property, String value) {
        return new PropertyDiff(property, null, value);
    }
}
