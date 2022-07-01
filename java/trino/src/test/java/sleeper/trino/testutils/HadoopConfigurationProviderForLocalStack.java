package sleeper.trino.testutils;

import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.trino.remotesleeperconnection.HadoopConfigurationProvider;

import static java.util.Objects.requireNonNull;

public class HadoopConfigurationProviderForLocalStack implements HadoopConfigurationProvider {
    private final LocalStackContainer localStackContainer;

    public HadoopConfigurationProviderForLocalStack(LocalStackContainer localStackContainer) {
        this.localStackContainer = requireNonNull(localStackContainer);
    }

    @Override
    public Configuration getHadoopConfiguration(InstanceProperties instanceProperties) {
        Configuration configuration = new Configuration();
        configuration.setClassLoader(this.getClass().getClassLoader());
        configuration.set("fs.s3a.endpoint", localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3).getServiceEndpoint());
        configuration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        configuration.set("fs.s3a.access.key", localStackContainer.getAccessKey());
        configuration.set("fs.s3a.secret.key", localStackContainer.getSecretKey());
        configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
        // The following settings may be useful if the connection to the localstack S3 instance hangs.
        // These settings attempt to force connection issues to generate errors ealy.
        // The settings do help but errors mayn still take many minutes to appear.
        // configuration.set("fs.s3a.connection.timeout", "1000");
        // configuration.set("fs.s3a.connection.establish.timeout", "1");
        // configuration.set("fs.s3a.attempts.maximum", "1");
        return configuration;
    }
}
