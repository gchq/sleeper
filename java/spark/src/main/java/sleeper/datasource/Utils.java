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
package sleeper.datasource;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

import static sleeper.core.properties.PropertiesUtils.loadProperties;

public class Utils {

    public static String saveInstancePropertiesToString(InstanceProperties instanceProperties) {
        return instanceProperties.saveAsString();
    }

    public static InstanceProperties loadInstancePropertiesFromString(String instancePropertiesString) {
        return InstanceProperties.createWithoutValidation(loadProperties(instancePropertiesString));
    }

    public static String saveTablePropertiesToString(TableProperties tableProperties) {
        return tableProperties.saveAsString();
    }

    public static TableProperties loadTablePropertiesFromString(InstanceProperties instanceProperties, String tablePropertiesString) {
        return new TableProperties(instanceProperties, loadProperties(tablePropertiesString));
    }
}
