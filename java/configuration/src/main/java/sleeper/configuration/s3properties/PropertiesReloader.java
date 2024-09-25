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

package sleeper.configuration.s3properties;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;

import static sleeper.configuration.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;

/**
 * Reloads cached configuration properties when trigged if configured to do so.
 */
public interface PropertiesReloader {

    /**
     * Triggers reloading instance and/or table properties if needed.
     */
    void reloadIfNeeded();

    /**
     * Creates a properties reloader that will never reload any properties.
     *
     * @return the reloader
     */
    static PropertiesReloader neverReload() {
        return () -> {
        };
    }

    /**
     * Creates a properties reloader that will reload all properties if an instance property is set to force reload.
     *
     * @param  s3Client                the S3 client
     * @param  instanceProperties      the instance properties to reload
     * @param  tablePropertiesProvider the table properties cache to clear
     * @return                         the reloader
     */
    static PropertiesReloader ifConfigured(
            AmazonS3 s3Client, InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        return () -> {
            if (instanceProperties.getBoolean(FORCE_RELOAD_PROPERTIES)) {
                S3InstanceProperties.reload(s3Client, instanceProperties);
                if (tablePropertiesProvider != null) {
                    tablePropertiesProvider.clearCache();
                }
            }
        };
    }

    /**
     * Creates a properties reloader that will reload instance properties if a property is set to force reload. This
     * should only be used in a context where table properties are not used.
     *
     * @param  s3Client           the S3 client
     * @param  instanceProperties the instance properties to reload
     * @return                    the reloader
     */
    static PropertiesReloader ifConfigured(
            AmazonS3 s3Client, InstanceProperties instanceProperties) {
        return ifConfigured(s3Client, instanceProperties, null);
    }

    /**
     * Creates a properties reloader that will always reload instance properties. This should only be used in a context
     * where table properties are not used, and reloading instance properties is very important.
     *
     * @param  s3Client           the S3 client
     * @param  instanceProperties the instance properties to reload
     * @return                    the reloader
     */
    static PropertiesReloader alwaysReload(AmazonS3 s3Client, InstanceProperties instanceProperties) {
        return () -> {
            S3InstanceProperties.reload(s3Client, instanceProperties);
        };
    }
}
