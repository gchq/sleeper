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
package sleeper.clients.api.aws;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.parquet.utils.HadoopConfigurationProvider;

/**
 * Provides a Hadoop configuration to instantiate a Sleeper client.
 */
@FunctionalInterface
public interface SleeperClientHadoopProvider {

    /**
     * Creates or retrieves the Hadoop configuration.
     *
     * @param  instanceProperties the Sleeper instance properties
     * @return                    the Hadoop configuration
     */
    Configuration getConfiguration(InstanceProperties instanceProperties);

    /**
     * Creates a provider that will apply the default client configuration for a given Sleeper instance.
     *
     * @return the provider
     */
    static SleeperClientHadoopProvider getDefault() {
        return instanceProperties -> HadoopConfigurationProvider.getConfigurationForClient(instanceProperties);
    }

    /**
     * Creates a provider that will reuse the given configuration.
     *
     * @param  configuration the Hadoop configuration
     * @return               the provider
     */
    static SleeperClientHadoopProvider withConfig(Configuration configuration) {
        return instanceProperties -> configuration;
    }

}
