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

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;

public interface PropertiesReloader {
    void reloadIfNeeded();

    static PropertiesReloader neverReload() {
        return () -> {
        };
    }

    static PropertiesReloader ifConfigured(
            AmazonS3 s3Client, InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        return () -> {
            if (instanceProperties.getBoolean(FORCE_RELOAD_PROPERTIES)) {
                instanceProperties.loadFromS3(s3Client, instanceProperties.get(CONFIG_BUCKET));
                if (tablePropertiesProvider != null) {
                    tablePropertiesProvider.clearCache();
                }
            }
        };
    }

    static PropertiesReloader ifConfigured(
            AmazonS3 s3Client, InstanceProperties instanceProperties) {
        return ifConfigured(s3Client, instanceProperties, null);
    }
}
