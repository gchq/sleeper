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
package sleeper.configurationv2.properties;

import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.properties.PropertiesReloader;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TablePropertiesProvider;

import static sleeper.core.properties.instance.CommonProperty.FORCE_RELOAD_PROPERTIES;

/**
 * Reloads cached configuration properties from S3 when trigged if configured to do so.
 */
public class S3PropertiesReloader implements PropertiesReloader {

    private final S3Client s3Client;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;

    private S3PropertiesReloader(S3Client s3Client, InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        this.s3Client = s3Client;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    /**
     * Creates a properties reloader that will reload all properties if an instance property is set to force reload.
     *
     * @param  s3Client                the S3 client
     * @param  instanceProperties      the instance properties to reload
     * @param  tablePropertiesProvider the table properties cache to clear
     * @return                         the reloader
     */
    public static PropertiesReloader ifConfigured(
            S3Client s3Client, InstanceProperties instanceProperties, TablePropertiesProvider tablePropertiesProvider) {
        return new S3PropertiesReloader(s3Client, instanceProperties, tablePropertiesProvider);
    }

    /**
     * Creates a properties reloader that will reload instance properties if a property is set to force reload. This
     * should only be used in a context where table properties are not used.
     *
     * @param  s3Client           the S3 client
     * @param  instanceProperties the instance properties to reload
     * @return                    the reloader
     */
    public static PropertiesReloader ifConfigured(
            S3Client s3Client, InstanceProperties instanceProperties) {
        return new S3PropertiesReloader(s3Client, instanceProperties, null);
    }

    @Override
    public void reloadIfNeeded() {
        if (instanceProperties.getBoolean(FORCE_RELOAD_PROPERTIES)) {
            S3InstanceProperties.reload(s3Client, instanceProperties);
            if (tablePropertiesProvider != null) {
                tablePropertiesProvider.clearCache();
            }
        }
    }

}
