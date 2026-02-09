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

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static java.util.Objects.requireNonNull;

/**
 * The entry point for this plugin. It provides the Trino framework with a
 * {@link sleeper.trino.SleeperConnectorFactory}.
 * It allows a Guice module to be provided during construction, if desired, to allow a different set of Guice injections
 * during testing. A {@link SleeperModule} is used by default.
 */
public final class SleeperPlugin implements Plugin {
    private final Module guiceModule;

    public SleeperPlugin() {
        this.guiceModule = new SleeperModule();
    }

    public SleeperPlugin(Module guiceModule) {
        this.guiceModule = requireNonNull(guiceModule);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new SleeperConnectorFactory(guiceModule));
    }
}
