/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.clients.admin.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.container.UploadDockerImagesToEcrRequest;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.configuration.properties.S3TableProperties;
import sleeper.configuration.table.index.DynamoDBTableIndex;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.instance.InstanceProperty;
import sleeper.core.properties.local.SaveLocalProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.properties.table.TablePropertiesStore;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableIndex;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class AdminClientPropertiesStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientPropertiesStore.class);

    private final Client client;
    private final InvokeCdk cdk;
    private final DockerImageConfiguration dockerImageConfiguration;
    private final UploadDockerImagesToEcr uploadDockerImages;
    private final Path generatedDirectory;

    public AdminClientPropertiesStore(
            String accountName, S3Client s3Client, DynamoDbClient dynamoClient, InvokeCdk cdk, Path generatedDirectory,
            UploadDockerImagesToEcr uploadDockerImages, DockerImageConfiguration dockerImageConfiguration) {
        this(new AwsClient(accountName, s3Client, dynamoClient, generatedDirectory), cdk, generatedDirectory, uploadDockerImages, dockerImageConfiguration);
    }

    public AdminClientPropertiesStore(
            Client client, InvokeCdk cdk, Path generatedDirectory,
            UploadDockerImagesToEcr uploadDockerImages, DockerImageConfiguration dockerImageConfiguration) {
        this.client = client;
        this.dockerImageConfiguration = dockerImageConfiguration;
        this.uploadDockerImages = uploadDockerImages;
        this.cdk = cdk;
        this.generatedDirectory = generatedDirectory;
    }

    public InstanceProperties loadInstanceProperties(String instanceId) throws CouldNotLoadInstanceProperties {
        try {
            return client.loadInstancePropertiesNoValidation(instanceId);
        } catch (RuntimeException e) {
            throw new CouldNotLoadInstanceProperties(instanceId, e);
        }
    }

    public TableIndex loadTableIndex(String instanceId) throws CouldNotLoadInstanceProperties {
        return client.createTableIndex(loadInstanceProperties(instanceId));
    }

    public TableProperties loadTableProperties(InstanceProperties instanceProperties, String tableName) throws CouldNotLoadTableProperties {
        try {
            return client.createTablePropertiesStore(instanceProperties)
                    .loadByNameNoValidation(tableName);
        } catch (RuntimeException e) {
            throw new CouldNotLoadTableProperties(instanceProperties.get(ID), tableName, e);
        }
    }

    private Stream<TableProperties> streamTableProperties(InstanceProperties instanceProperties) {
        return client.createTablePropertiesStore(instanceProperties).streamAllTables();
    }

    public void saveInstanceProperties(InstanceProperties properties, PropertiesDiff diff) {
        try {
            LOGGER.info("Saving to local configuration");
            client.saveLocalProperties(properties, streamTableProperties(properties));
            List<InstanceProperty> propertiesDeployedByCdk = diff.getChangedPropertiesDeployedByCDK(properties.getPropertiesIndex());
            if (!propertiesDeployedByCdk.isEmpty()) {
                uploadDockerImages.upload(UploadDockerImagesToEcrRequest.forDeployment(properties, dockerImageConfiguration));
                LOGGER.info("Deploying by CDK, properties requiring CDK deployment: {}", propertiesDeployedByCdk);
                cdk.invokeInferringType(properties, CdkCommand.deployPropertiesChange(generatedDirectory.resolve("instance.properties")));
            } else {
                LOGGER.info("Saving to AWS");
                client.saveInstanceProperties(properties);
            }
        } catch (IOException | RuntimeException | InterruptedException e) {
            String instanceId = properties.get(ID);
            CouldNotSaveInstanceProperties wrapped = new CouldNotSaveInstanceProperties(instanceId, e);
            try {
                LOGGER.info("Reverting local configuration");
                client.saveLocalProperties(loadInstanceProperties(instanceId), streamTableProperties(properties));
            } catch (Exception e2) {
                wrapped.addSuppressed(e2);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw wrapped;
        }
    }

    public void saveTableProperties(String instanceId, TableProperties properties) {
        saveTableProperties(loadInstanceProperties(instanceId), properties);
    }

    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties properties) {
        String instanceId = instanceProperties.get(ID);
        String tableName = properties.get(TABLE_NAME);
        try {
            LOGGER.info("Saving to local configuration");
            client.saveLocalProperties(instanceProperties,
                    streamTableProperties(instanceProperties)
                            .map(table -> tableName.equals(table.get(TABLE_NAME)) ? properties : table));
            LOGGER.info("Saving to AWS");
            client.createTablePropertiesStore(instanceProperties).save(properties);
        } catch (IOException | RuntimeException e) {
            CouldNotSaveTableProperties wrapped = new CouldNotSaveTableProperties(instanceId, tableName, e);
            try {
                LOGGER.info("Reverting local configuration");
                client.saveLocalProperties(loadInstanceProperties(instanceId), streamTableProperties(instanceProperties));
            } catch (Exception e2) {
                wrapped.addSuppressed(e2);
            }
            throw wrapped;
        }
    }

    public StateStore loadStateStore(String instanceId, TableProperties tableProperties) {
        InstanceProperties instanceProperties = loadInstanceProperties(instanceId);
        return client.createStateStore(instanceProperties, tableProperties);
    }

    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties properties) {
        return new TablePropertiesProvider(properties, client.createTablePropertiesStore(properties));
    }

    public static class CouldNotLoadInstanceProperties extends CouldNotLoadProperties {
        public CouldNotLoadInstanceProperties(String instanceId, Throwable cause) {
            super("Could not load properties for instance " + instanceId, cause);
        }
    }

    public static class CouldNotSaveInstanceProperties extends CouldNotSaveProperties {
        public CouldNotSaveInstanceProperties(String instanceId, Throwable cause) {
            super("Could not save properties for instance " + instanceId, cause);
        }
    }

    public static class CouldNotLoadTableProperties extends CouldNotLoadProperties {
        public CouldNotLoadTableProperties(String instanceId, String tableName, Throwable cause) {
            super("Could not load properties for table " + tableName + " in instance " + instanceId, cause);
        }
    }

    public static class CouldNotSaveTableProperties extends CouldNotSaveProperties {
        public CouldNotSaveTableProperties(String instanceId, String tableName, Throwable cause) {
            super("Could not save properties for table " + tableName + " in instance " + instanceId, cause);
        }
    }

    public static class CouldNotLoadProperties extends ConfigStoreException {
        public CouldNotLoadProperties(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class CouldNotSaveProperties extends ConfigStoreException {
        public CouldNotSaveProperties(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class ConfigStoreException extends RuntimeException {
        public ConfigStoreException(String message, Throwable cause) {
            super(message, cause);
        }

        public void print(ConsoleOutput out) {
            out.println(getMessage());
            out.println("Cause: " + getCause().getMessage());
        }
    }

    /**
     * A client to interact with the underlying storage of configuration properties.
     */
    public interface Client {

        /**
         * Loads instance properties without validation.
         *
         * @param  instanceId the instance ID
         * @return            the instance properties
         */
        InstanceProperties loadInstancePropertiesNoValidation(String instanceId);

        /**
         * Saves instance properties without validation.
         *
         * @param instanceProperties the instance properties
         */
        void saveInstanceProperties(InstanceProperties instanceProperties);

        /**
         * Saves configuration properties to a local directory, rather than the underlying store.
         *
         * @param  instanceProperties    the instance properties
         * @param  tablePropertiesStream a stream of table properties
         * @throws IOException           thrown on a failure interacting with the file system
         */
        void saveLocalProperties(InstanceProperties instanceProperties, Stream<TableProperties> tablePropertiesStream) throws IOException;

        /**
         * Retrieves an object to interact with the table index of a Sleeper instance.
         *
         * @param  instanceProperties the instance properties
         * @return                    the table index
         */
        TableIndex createTableIndex(InstanceProperties instanceProperties);

        /**
         * Retrieves an object to interact with the table properties of a Sleeper instance.
         *
         * @param  instanceProperties the instance properties
         * @return                    the table properties store
         */
        TablePropertiesStore createTablePropertiesStore(InstanceProperties instanceProperties);

        /**
         * Retrieves an object to interact with the state store for a table.
         *
         * @param  instanceProperties the instance properties
         * @param  tableProperties    the table properties
         * @return                    the state store
         */
        StateStore createStateStore(InstanceProperties instanceProperties, TableProperties tableProperties);
    }

    /**
     * A client to interact with configuration in S3, DynamoDB and a local directory.
     */
    public static class AwsClient implements Client {

        private final String accountName;
        private final S3Client s3Client;
        private final DynamoDbClient dynamoClient;
        private final Path localDirectory;

        public AwsClient(String accountName, S3Client s3Client, DynamoDbClient dynamoClient, Path localDirectory) {
            this.accountName = accountName;
            this.s3Client = s3Client;
            this.dynamoClient = dynamoClient;
            this.localDirectory = localDirectory;
        }

        @Override
        public InstanceProperties loadInstancePropertiesNoValidation(String instanceId) {
            return S3InstanceProperties.loadGivenAccountAndInstanceIdNoValidation(s3Client, accountName, instanceId);
        }

        @Override
        public void saveInstanceProperties(InstanceProperties instanceProperties) {
            S3InstanceProperties.saveToS3(s3Client, instanceProperties);
        }

        @Override
        public void saveLocalProperties(InstanceProperties instanceProperties, Stream<TableProperties> tablePropertiesStream) throws IOException {
            Files.createDirectories(localDirectory);
            ClientUtils.clearDirectory(localDirectory);
            SaveLocalProperties.saveToDirectory(localDirectory, instanceProperties, tablePropertiesStream);
        }

        @Override
        public TableIndex createTableIndex(InstanceProperties instanceProperties) {
            return new DynamoDBTableIndex(instanceProperties, dynamoClient);
        }

        @Override
        public TablePropertiesStore createTablePropertiesStore(InstanceProperties instanceProperties) {
            return S3TableProperties.createStore(instanceProperties, s3Client, dynamoClient);
        }

        @Override
        public StateStore createStateStore(InstanceProperties instanceProperties, TableProperties tableProperties) {
            return new StateStoreFactory(instanceProperties, s3Client, dynamoClient)
                    .getStateStore(tableProperties);
        }

    }
}
