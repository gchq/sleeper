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
 * A factory class to create {@link SleeperConnector} objects.
 * <p>
 * This implementation uses Guice to perform dependency-injection and construct the classes which are required by the
 * resulting Connector object. Guice is configured within {@link SleeperModule} The module is passed in during
 * construction so that a different module may be used during testing.
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
