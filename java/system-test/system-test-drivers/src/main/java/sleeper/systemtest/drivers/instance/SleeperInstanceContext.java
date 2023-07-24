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
import sleeper.configuration.properties.table.TableProperty;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class SleeperInstanceContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceContext.class);

    private final SystemTestParameters parameters;
    private final CloudFormationClient cloudFormationClient;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final DeployedInstances deployed = new DeployedInstances();
    private Instance currentInstance;

    public SleeperInstanceContext(SystemTestParameters parameters,
                                  CloudFormationClient cloudFormationClient,
                                  AmazonS3 s3Client,
                                  AmazonDynamoDB dynamoDBClient) {
        this.parameters = parameters;
        this.cloudFormationClient = cloudFormationClient;
        this.s3Client = s3Client;
        this.dynamoDBClient = dynamoDBClient;
    }

    public void connectTo(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
        currentInstance = deployed.connectTo(identifier, deployInstanceConfiguration);
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
                DeployNewInstance.builder().scriptsDirectory(parameters.getScriptsDirectory())
                        .deployInstanceConfiguration(deployInstanceConfiguration)
                        .instanceId(instanceId)
                        .vpcId(parameters.getVpcId())
                        .subnetIds(parameters.getSubnetIds())
                        .deployPaused(true)
                        .tableName(tableName)
                        .instanceType(InvokeCdkForInstance.Type.STANDARD)
                        .runCommand(ClientUtils::runCommandLogOutput)
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
            TableProperties tableProperties = new TableProperties(instanceProperties);
            tableProperties.loadFromS3(s3Client, tableName);
            StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDBClient, instanceProperties);
            return new Instance(instanceProperties, tableProperties, stateStoreProvider);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private class DeployedInstances {
        private final Map<String, Instance> instanceById = new HashMap<>();

        public Instance connectTo(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
            return instanceById.computeIfAbsent(identifier,
                    id -> createInstanceIfMissing(id, deployInstanceConfiguration));
        }
    }

    public static class Instance {
        private final InstanceProperties instanceProperties;
        private final TableProperties tableProperties;
        private final StateStoreProvider stateStoreProvider;

        public Instance(InstanceProperties instanceProperties, TableProperties tableProperties, StateStoreProvider stateStoreProvider) {
            this.instanceProperties = instanceProperties;
            this.tableProperties = tableProperties;
            this.stateStoreProvider = stateStoreProvider;
        }

        public InstanceProperties getInstanceProperties() {
            return instanceProperties;
        }

        public TableProperties getTableProperties() {
            return tableProperties;
        }

        public StateStoreProvider getStateStoreProvider() {
            return stateStoreProvider;
        }
    }

}
