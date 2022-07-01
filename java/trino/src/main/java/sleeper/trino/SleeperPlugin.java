package sleeper.trino;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static java.util.Objects.requireNonNull;

/**
 * The entry point for this plugin. It provides the Trino framework with a {@link sleeper.trino.SleeperConnectorFactory}.
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
