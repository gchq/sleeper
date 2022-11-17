/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest;

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.record.Record;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;

public class IngestFactory {

    private final ObjectFactory objectFactory;
    private final String localDir;
    private final StateStoreProvider stateStoreProvider;
    private final InstanceProperties instanceProperties;
    private final Configuration hadoopConfiguration;
    private final S3AsyncClient s3AsyncClient;

    private IngestFactory(Builder builder) {
        objectFactory = Objects.requireNonNull(builder.objectFactory, "objectFactory must not be null");
        localDir = Objects.requireNonNull(builder.localDir, "localDir must not be null");
        stateStoreProvider = Objects.requireNonNull(builder.stateStoreProvider, "stateStoreProvider must not be null");
        instanceProperties = Objects.requireNonNull(builder.instanceProperties, "instanceProperties must not be null");
        if (builder.hadoopConfiguration == null) {
            hadoopConfiguration = defaultHadoopConfiguration();
        } else {
            hadoopConfiguration = builder.hadoopConfiguration;
        }
        // If S3AsyncClient is not set, a default client will be created if it is needed.
        s3AsyncClient = builder.s3AsyncClient;
    }

    public IngestResult ingestFromRecordIterator(TableProperties tableProperties, CloseableIterator<Record> recordIterator)
            throws StateStoreException, IteratorException, IOException {
        return IngestRecordsUsingPropertiesSpecifiedMethod.ingestFromRecordIterator(
                objectFactory,
                stateStoreProvider.getStateStore(tableProperties),
                instanceProperties,
                tableProperties,
                localDir,
                s3AsyncClient,
                hadoopConfiguration,
                tableProperties.get(ITERATOR_CLASS_NAME),
                tableProperties.get(ITERATOR_CONFIG),
                recordIterator);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a simple default Hadoop configuration which may be used if no other configuration is provided.
     *
     * @return The Hadoop configuration
     */
    private static Configuration defaultHadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.fast.upload", "true");
        return conf;
    }

    public static final class Builder {
        private ObjectFactory objectFactory;
        private String localDir;
        private StateStoreProvider stateStoreProvider;
        private InstanceProperties instanceProperties;
        private Configuration hadoopConfiguration;
        private S3AsyncClient s3AsyncClient;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder localDir(String localDir) {
            this.localDir = localDir;
            return this;
        }

        public Builder stateStoreProvider(StateStoreProvider stateStoreProvider) {
            this.stateStoreProvider = stateStoreProvider;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        /**
         * The client to use for asynchronous S3 operations.
         * This may or may not be used depending on the settings for an ingest.
         * If this is not set, a default client will be created if it is needed.
         *
         * @param s3AsyncClient The client to use
         * @return The builder for chaining calls
         */
        public Builder s3AsyncClient(S3AsyncClient s3AsyncClient) {
            this.s3AsyncClient = s3AsyncClient;
            return this;
        }

        public IngestFactory build() {
            return new IngestFactory(this);
        }
    }
}
