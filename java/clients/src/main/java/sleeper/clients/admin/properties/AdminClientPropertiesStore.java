/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.deploy.DockerImageConfiguration;
import sleeper.clients.deploy.StacksForDockerUpload;
import sleeper.clients.deploy.UploadDockerImages;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.clients.util.console.ConsoleOutput;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.InstanceProperty;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.s3properties.S3InstanceProperties;
import sleeper.configuration.s3properties.S3TableProperties;
import sleeper.core.properties.validation.OptionalStack;
import sleeper.core.properties.validation.SleeperPropertyValueUtils;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableNotFoundException;
import sleeper.statestore.StateStoreFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AdminClientPropertiesStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientPropertiesStore.class);

    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final InvokeCdkForInstance cdk;
    private final DockerImageConfiguration dockerImageConfiguration;
    private final UploadDockerImages uploadDockerImages;
    private final Path generatedDirectory;

    public AdminClientPropertiesStore(
            AmazonS3 s3, AmazonDynamoDB dynamoDB, InvokeCdkForInstance cdk,
            Path generatedDirectory, UploadDockerImages uploadDockerImages) {
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.dockerImageConfiguration = new DockerImageConfiguration();
        this.uploadDockerImages = uploadDockerImages;
        this.cdk = cdk;
        this.generatedDirectory = generatedDirectory;
    }

    public InstanceProperties loadInstanceProperties(String instanceId) {
        try {
            return S3InstanceProperties.loadGivenInstanceIdNoValidation(s3, instanceId);
        } catch (AmazonS3Exception e) {
            throw new CouldNotLoadInstanceProperties(instanceId, e);
        }
    }

    public TableProperties loadTableProperties(InstanceProperties instanceProperties, String tableName) {
        try {
            return S3TableProperties.getStore(instanceProperties, s3, dynamoDB)
                    .loadByNameNoValidation(tableName);
        } catch (TableNotFoundException e) {
            throw new CouldNotLoadTableProperties(instanceProperties.get(ID), tableName, e);
        }
    }

    private Stream<TableProperties> streamTableProperties(InstanceProperties instanceProperties) {
        return S3TableProperties.getStore(instanceProperties, s3, dynamoDB).streamAllTables();
    }

    public void saveInstanceProperties(InstanceProperties properties, PropertiesDiff diff) {
        try {
            LOGGER.info("Saving to local configuration");
            Files.createDirectories(generatedDirectory);
            ClientUtils.clearDirectory(generatedDirectory);
            SaveLocalProperties.saveToDirectory(generatedDirectory, properties, streamTableProperties(properties));
            if (shouldUploadDockerImages(diff)) {
                LOGGER.info("New stack has been added which requires a docker image. Uploading missing images.");
                uploadDockerImages.upload(StacksForDockerUpload.from(properties));
            }
            List<InstanceProperty> propertiesDeployedByCdk = diff.getChangedPropertiesDeployedByCDK(properties.getPropertiesIndex());
            if (!propertiesDeployedByCdk.isEmpty()) {
                LOGGER.info("Deploying by CDK, properties requiring CDK deployment: {}", propertiesDeployedByCdk);
                cdk.invokeInferringType(properties, CdkCommand.deployPropertiesChange());
            } else {
                LOGGER.info("Saving to AWS");
                S3InstanceProperties.saveToS3(s3, properties);
            }
        } catch (IOException | AmazonS3Exception | InterruptedException e) {
            String instanceId = properties.get(ID);
            CouldNotSaveInstanceProperties wrapped = new CouldNotSaveInstanceProperties(instanceId, e);
            try {
                S3InstanceProperties.saveToLocalWithTableProperties(s3, dynamoDB, instanceId, generatedDirectory);
            } catch (Exception e2) {
                wrapped.addSuppressed(e2);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw wrapped;
        }
    }

    private boolean shouldUploadDockerImages(PropertiesDiff diff) {
        Optional<PropertyDiff> stackDiffOptional = diff.getChanges().stream()
                .filter(propertyDiff -> propertyDiff.getPropertyName().equals(OPTIONAL_STACKS.getPropertyName()))
                .findFirst();
        if (stackDiffOptional.isEmpty()) {
            return false;
        }
        PropertyDiff stackDiff = stackDiffOptional.get();
        Set<OptionalStack> stacksBefore = SleeperPropertyValueUtils
                .streamEnumList(OPTIONAL_STACKS, stackDiff.getOldValue(), OptionalStack.class)
                .collect(toUnmodifiableSet());
        Set<OptionalStack> newStacks = SleeperPropertyValueUtils
                .streamEnumList(OPTIONAL_STACKS, stackDiff.getNewValue(), OptionalStack.class)
                .filter(not(stacksBefore::contains))
                .collect(toUnmodifiableSet());
        return !dockerImageConfiguration.getStacksToDeploy(newStacks).isEmpty();
    }

    public void saveTableProperties(String instanceId, TableProperties properties) {
        saveTableProperties(loadInstanceProperties(instanceId), properties);
    }

    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties properties) {
        String instanceId = instanceProperties.get(ID);
        String tableName = properties.get(TABLE_NAME);
        try {
            LOGGER.info("Saving to local configuration");
            Files.createDirectories(generatedDirectory);
            ClientUtils.clearDirectory(generatedDirectory);
            SaveLocalProperties.saveToDirectory(generatedDirectory, instanceProperties,
                    streamTableProperties(instanceProperties)
                            .map(table -> tableName.equals(table.get(TABLE_NAME)) ? properties : table));
            LOGGER.info("Saving to AWS");
            S3TableProperties.getStore(instanceProperties, s3, dynamoDB).save(properties);
        } catch (IOException | AmazonS3Exception e) {
            CouldNotSaveTableProperties wrapped = new CouldNotSaveTableProperties(instanceId, tableName, e);
            try {
                S3InstanceProperties.saveToLocalWithTableProperties(s3, dynamoDB, instanceId, generatedDirectory);
            } catch (Exception e2) {
                wrapped.addSuppressed(e2);
            }
            throw wrapped;
        }
    }

    public StateStore loadStateStore(String instanceId, TableProperties tableProperties) {
        InstanceProperties instanceProperties = loadInstanceProperties(instanceId);
        StateStoreFactory stateStoreFactory = new StateStoreFactory(instanceProperties, s3, dynamoDB, new Configuration());
        return stateStoreFactory.getStateStore(tableProperties);
    }

    public TablePropertiesProvider createTablePropertiesProvider(InstanceProperties properties) {
        return S3TableProperties.createProvider(properties, s3, dynamoDB);
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
}
