package sleeper.trino.remotesleeperconnection;

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.properties.InstanceProperties;

public interface HadoopConfigurationProvider {
    Configuration getHadoopConfiguration(InstanceProperties instanceProperties);
}
