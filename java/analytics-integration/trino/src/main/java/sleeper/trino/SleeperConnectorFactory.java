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
package sleeper.trino;

import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A factory to create Sleeper connectors. Uses Guice to perform dependency injection and construct the dependencies of
 * the connector.
 * <p>
 * Guice is configured within {@link SleeperModule}. The module is passed in during construction so that a different
 * module may be used during testing.
 */
public class SleeperConnectorFactory implements ConnectorFactory {
    private final Module guiceModule;

    public SleeperConnectorFactory(Module guiceModule) {
        this.guiceModule = requireNonNull(guiceModule);
    }

    @Override
    public String getName() {
        return "sleeper";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context) {
        requireNonNull(catalogName);
        requireNonNull(requiredConfig);
        requireNonNull(context);

        // This is where the Guice magic takes place
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                guiceModule);
        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(SleeperConnector.class);
    }
}
