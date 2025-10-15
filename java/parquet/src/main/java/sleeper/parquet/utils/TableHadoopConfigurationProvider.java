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
package sleeper.parquet.utils;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;

/**
 * A provider to retrieve Hadoop configuration specific to a Sleeper table.
 */
public interface TableHadoopConfigurationProvider {

    /**
     * Retreives the Hadoop configuration for a Sleeper table.
     *
     * @param  tableProperties the table properties
     * @return                 the Hadoop configuration
     */
    Configuration getConfiguration(TableProperties tableProperties);

    /**
     * Creates a provider that always returns a fixed configuration.
     *
     * @param  hadoopConf the Hadoop configuration
     * @return            the provider
     */
    static TableHadoopConfigurationProvider fixed(Configuration hadoopConf) {
        return tableProperties -> hadoopConf;
    }

    /**
     * Creates a provider that creates a new configuration every time, for a client.
     *
     * @param  instanceProperties the instance properties
     * @return                    the provider
     */
    static TableHadoopConfigurationProvider forClient(InstanceProperties instanceProperties) {
        return tableProperties -> HadoopConfigurationProvider.getConfigurationForClient(instanceProperties, tableProperties);
    }
}
