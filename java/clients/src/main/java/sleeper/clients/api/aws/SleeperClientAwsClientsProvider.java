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

import java.util.function.Consumer;

/**
 * Provides AWS clients to instantiate a Sleeper client.
 */
@FunctionalInterface
public interface SleeperClientAwsClientsProvider {

    /**
     * Creates or retrieves the AWS clients.
     *
     * @return the AWS clients
     */
    SleeperClientAwsClients getAwsClients();

    /**
     * Creates a provider that will create AWS clients with the default configuration. These will be created separately
     * for each Sleeper client and closed when the Sleeper client is closed.
     *
     * @return the provider
     */
    static SleeperClientAwsClientsProvider createDefaultForEachClient() {
        return () -> SleeperClientAwsClients.builder().defaultClients().build();
    }

    /**
     * Creates a provider that will apply the given configuration.
     *
     * @param  config the configuration to apply
     * @return        the provider
     */
    static SleeperClientAwsClientsProvider withConfig(Consumer<SleeperClientAwsClients.Builder> config) {
        return () -> {
            SleeperClientAwsClients.Builder builder = SleeperClientAwsClients.builder();
            config.accept(builder);
            return builder.build();
        };
    }

    /**
     * Creates a provider that will reuse the given clients.
     *
     * @param  clients the AWS clients
     * @return         the provider
     */
    static SleeperClientAwsClientsProvider withClients(SleeperClientAwsClients clients) {
        return () -> clients;
    }

}
