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
package sleeper.bulkimport.configuration;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.configuration.properties.validation.SleeperPropertyValueUtils;

import java.util.List;
import java.util.Map;

public class BulkImportPlatformSpec {

    private final TableProperties tableProperties;
    private final Map<String, String> platformSpec;

    public BulkImportPlatformSpec(TableProperties tableProperties, BulkImportJob job) {
        this.tableProperties = tableProperties;
        this.platformSpec = job.getPlatformSpec();
    }

    public String get(TableProperty property) {
        if (null == platformSpec) {
            return tableProperties.get(property);
        }
        return platformSpec.getOrDefault(property.getPropertyName(), tableProperties.get(property));
    }

    public boolean getBoolean(TableProperty property) {
        return Boolean.parseBoolean(get(property));
    }

    public int getInt(TableProperty property) {
        return Integer.parseInt(get(property));
    }

    public List<String> getList(TableProperty property) {
        return SleeperPropertyValueUtils.readList(get(property));
    }

    public String getOrDefault(TableProperty property, String defaultValue) {
        String value = get(property);
        if (value == null) {
            return defaultValue;
        } else {
            return value;
        }
    }

    public TableProperties getTableProperties() {
        return tableProperties;
    }
}
