/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.status.update.ReinitialiseTable;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.instance.CommonProperty;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import static sleeper.configuration.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;
import static sleeper.systemtest.drivers.instance.OutputInstanceIds.addInstanceIdToOutput;

public class SleeperInstanceContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceContext.class);

    private final SystemTestParameters parameters;
    private final SystemTestInstanceContext systemTest;
    private final CloudFormationClient cloudFormationClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final DeployedInstances deployed = new DeployedInstances();
    private Instance currentInstance;

    public SleeperInstanceContext(SystemTestParameters parameters,
                                  SystemTestInstanceContext systemTest,
                                  CloudFormationClient cloudFormationClient,
                                  AmazonS3 s3Client,
                                  AmazonDynamoDB dynamoDBClient) {
        this.parameters = parameters;
        this.systemTest = systemTest;
        this.cloudFormationClient = cloudFormationClient;
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
    }

    public void connectTo(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
        currentInstance = deployed.connectTo(identifier, deployInstanceConfiguration);
    }

    public void disconnect() {
        currentInstance = null;
    }

    public void resetProperties(DeployInstanceConfiguration configuration) {
        ResetProperties.reset(configuration,
                currentInstance.getInstanceProperties(),
                currentInstance.getTableProperties(),
                s3Client);
    }

    public void reinitialise() {
        try {
            new ReinitialiseTable(s3Client, dynamoDBClient,
                    currentInstance.getInstanceProperties().get(CommonProperty.ID),
                    currentInstance.getTableProperties().get(TableProperty.TABLE_NAME),
                    true).run();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (StateStoreException e) {
            throw new RuntimeException(e);
        }
    }

    public InstanceProperties getInstanceProperties() {
        return currentInstance.getInstanceProperties();
    }

    public TableProperties getTableProperties() {
        return currentInstance.getTableProperties();
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return currentInstance.getTablePropertiesProvider();
    }

    public StateStoreProvider getStateStoreProvider() {
        return currentInstance.getStateStoreProvider();
    }

    public StateStore getStateStore() {
        return getStateStoreProvider().getStateStore(getTableProperties());
    }

    public String getTableName() {
        return getTableProperties().get(TableProperty.TABLE_NAME);
    }

    private Instance createInstanceIfMissing(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
        String instanceId = parameters.buildInstanceId(identifier);
        String tableName = "system-test";
        try {
            cloudFormationClient.describeStacks(builder -> builder.stackName(instanceId));
            LOGGER.info("Instance already exists: {}", instanceId);
        } catch (CloudFormationException e) {
            LOGGER.info("Deploying instance: {}", instanceId);
            try {
                InstanceProperties properties = deployInstanceConfiguration.getInstanceProperties();
                properties.set(INGEST_SOURCE_BUCKET, systemTest.getSystemTestBucketName());
                properties.set(INGEST_SOURCE_ROLE, systemTest.getSystemTestWriterRoleName());
                properties.set(ECR_REPOSITORY_PREFIX, parameters.getSystemTestShortId());
                DeployNewInstance.builder().scriptsDirectory(parameters.getScriptsDirectory())
                        .deployInstanceConfiguration(deployInstanceConfiguration)
                        .instanceId(instanceId)
                        .vpcId(parameters.getVpcId())
                        .subnetIds(parameters.getSubnetIds())
                        .deployPaused(true)
                        .tableName(tableName)
                        .instanceType(InvokeCdkForInstance.Type.STANDARD)
                        .runCommand(ClientUtils::runCommandLogOutput)
                        .extraInstanceProperties(instanceProperties ->
                                instanceProperties.set(JARS_BUCKET, parameters.buildJarsBucketName()))
                        .deployWithDefaultClients();
            } catch (IOException ex) {
                throw new RuntimeIOException(ex);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
        try {
            InstanceProperties instanceProperties = new InstanceProperties();
            instanceProperties.loadFromS3GivenInstanceId(s3Client, instanceId);
            TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3Client, instanceProperties);
            TableProperties tableProperties = tablePropertiesProvider.getTableProperties(tableName);
            StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
            addInstanceIdToOutput(instanceId, parameters);
            return new Instance(instanceProperties, tableProperties, tablePropertiesProvider, stateStoreProvider);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private class DeployedInstances {
        private final Map<String, Exception> failureById = new HashMap<>();
        private final Map<String, Instance> instanceById = new HashMap<>();

        public Instance connectTo(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
            if (failureById.containsKey(identifier)) {
                throw new InstanceDidNotDeployException(identifier, failureById.get(identifier));
            }
            try {
                return instanceById.computeIfAbsent(identifier,
                        id -> createInstanceIfMissing(id, deployInstanceConfiguration));
            } catch (RuntimeException e) {
                failureById.put(identifier, e);
                throw e;
            }
        }
    }

    public static class Instance {
        private final InstanceProperties instanceProperties;
        private final TableProperties tableProperties;
        private final TablePropertiesProvider tablePropertiesProvider;
        private final StateStoreProvider stateStoreProvider;

        public Instance(InstanceProperties instanceProperties, TableProperties tableProperties,
                        TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
            this.instanceProperties = instanceProperties;
            this.tableProperties = tableProperties;
            this.tablePropertiesProvider = tablePropertiesProvider;
            this.stateStoreProvider = stateStoreProvider;
        }

        public InstanceProperties getInstanceProperties() {
            return instanceProperties;
        }

        public TableProperties getTableProperties() {
            return tableProperties;
        }

        public TablePropertiesProvider getTablePropertiesProvider() {
            return tablePropertiesProvider;
        }

        public StateStoreProvider getStateStoreProvider() {
            return stateStoreProvider;
        }
    }

}
