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
import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.DeployExistingInstance;
import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SleeperProperty;
import sleeper.configuration.properties.instance.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.SleeperVersion;
import sleeper.core.record.Record;
import sleeper.core.statestore.StateStore;
import sleeper.core.table.TableIdentity;
import sleeper.statestore.StateStoreProvider;
import sleeper.systemtest.datageneration.GenerateNumberedRecords;
import sleeper.systemtest.datageneration.GenerateNumberedValueOverrides;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.TAGS;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_BUCKET;
import static sleeper.configuration.properties.instance.IngestProperty.INGEST_SOURCE_ROLE;
import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class SleeperInstanceContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperInstanceContext.class);

    private final SystemTestParameters parameters;
    private final SystemTestDeploymentContext systemTest;
    private final AmazonDynamoDB dynamoDB;
    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AWSSecurityTokenService sts;
    private final AwsRegionProvider regionProvider;
    private final CloudFormationClient cloudFormationClient;
    private final AmazonECR ecr;
    private final DeployedInstances deployed = new DeployedInstances();
    private Instance currentInstance;

    public SleeperInstanceContext(SystemTestParameters parameters, SystemTestDeploymentContext systemTest,
                                  AmazonDynamoDB dynamoDB, AmazonS3 s3, S3Client s3v2,
                                  AWSSecurityTokenService sts, AwsRegionProvider regionProvider,
                                  CloudFormationClient cloudFormationClient, AmazonECR ecr) {
        this.parameters = parameters;
        this.systemTest = systemTest;
        this.dynamoDB = dynamoDB;
        this.s3 = s3;
        this.s3v2 = s3v2;
        this.sts = sts;
        this.regionProvider = regionProvider;
        this.cloudFormationClient = cloudFormationClient;
        this.ecr = ecr;
    }

    public void connectTo(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
        currentInstance = deployed.connectTo(identifier, deployInstanceConfiguration);
        currentInstance.setGeneratorOverrides(GenerateNumberedValueOverrides.none());
    }

    public void disconnect() {
        currentInstance = null;
    }

    public void resetPropertiesAndTables() {
        currentInstance = deployed.update(currentInstance.resetProperties().resetTables());
    }

    public void resetPropertiesAndDeleteTables() {
        currentInstance = deployed.update(currentInstance.resetProperties().deleteTables());
    }

    public void redeploy() throws InterruptedException {
        currentInstance = deployed.update(currentInstance.redeploy());
    }

    public InstanceProperties getInstanceProperties() {
        return currentInstance.getInstanceProperties();
    }

    public TableProperties getTableProperties() {
        return currentInstance.tables.getTableProperties();
    }

    public TablePropertiesProvider getTablePropertiesProvider() {
        return currentInstance.tables.getTablePropertiesProvider();
    }

    public void updateInstanceProperties(Map<UserDefinedInstanceProperty, String> values) {
        InstanceProperties instanceProperties = getInstanceProperties();
        values.forEach(instanceProperties::set);
        instanceProperties.saveToS3(s3);
    }

    public void updateTableProperties(Map<TableProperty, String> values) {
        TableProperties tableProperties = getTableProperties();
        List<TableProperty> uneditableProperties = values.keySet().stream()
                .filter(not(TableProperty::isEditable))
                .collect(Collectors.toUnmodifiableList());
        if (!uneditableProperties.isEmpty()) {
            throw new IllegalArgumentException("Cannot edit properties: " + uneditableProperties);
        }
        values.forEach(tableProperties::set);
        S3TableProperties.getStore(getInstanceProperties(), s3, dynamoDB)
                .save(tableProperties);
    }

    public StateStoreProvider getStateStoreProvider() {
        return currentInstance.tables.getStateStoreProvider();
    }

    public Stream<Record> generateNumberedRecords(LongStream numbers) {
        return currentInstance.generateNumberedRecords(numbers);
    }

    public StateStore getStateStore() {
        return getStateStoreProvider().getStateStore(getTableProperties());
    }

    public String getTableName() {
        return getTableProperties().get(TABLE_NAME);
    }

    public TableIdentity getTableId() {
        return getTableProperties().getId();
    }

    public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
        currentInstance.setGeneratorOverrides(overrides);
    }

    public void createTables(List<TableProperties> tableProperties) {
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

        public Instance update(Instance instance) {
            instanceById.put(instance.identifier, instance);
            return instance;
        }
    }

    private Instance createInstanceIfMissing(String identifier, DeployInstanceConfiguration deployInstanceConfiguration) {
        try {
            return createInstanceIfMissingOrThrow(identifier, deployInstanceConfiguration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Instance createInstanceIfMissingOrThrow(String identifier, DeployInstanceConfiguration deployConfig) throws InterruptedException, IOException {
        String instanceId = parameters.buildInstanceId(identifier);
        OutputInstanceIds.addInstanceIdToOutput(instanceId, parameters);
        try {
            cloudFormationClient.describeStacks(builder -> builder.stackName(instanceId));
            LOGGER.info("Instance already exists: {}", instanceId);
            return loadInstance(identifier, instanceId, deployConfig)
                    .redeployIfNeededNoDeployedUpdate();
        } catch (CloudFormationException e) {
            LOGGER.info("Deploying instance: {}", instanceId);
            InstanceProperties properties = deployConfig.getInstanceProperties();
            properties.set(INGEST_SOURCE_BUCKET, systemTest.getSystemTestBucketName());
            properties.set(INGEST_SOURCE_ROLE, systemTest.getSystemTestWriterRoleName());
            properties.set(ECR_REPOSITORY_PREFIX, parameters.getSystemTestShortId());
            if (parameters.getForceStateStoreClassname() != null) {
                for (TableProperties tableProperties : deployConfig.getTableProperties()) {
                    tableProperties.set(STATESTORE_CLASSNAME, parameters.getForceStateStoreClassname());
                }
            }
            DeployNewInstance.builder().scriptsDirectory(parameters.getScriptsDirectory())
                    .deployInstanceConfiguration(deployConfig)
                    .instanceId(instanceId)
                    .vpcId(parameters.getVpcId())
                    .subnetIds(parameters.getSubnetIds())
                    .deployPaused(true)
                    .instanceType(InvokeCdkForInstance.Type.STANDARD)
                    .runCommand(ClientUtils::runCommandLogOutput)
                    .extraInstanceProperties(instanceProperties ->
                            instanceProperties.set(JARS_BUCKET, parameters.buildJarsBucketName()))
                    .deployWithClients(sts, regionProvider, s3, s3v2, ecr, dynamoDB);
            return loadInstance(identifier, instanceId, deployConfig);
        }
    }

    private Instance loadInstance(String identifier, String instanceId, DeployInstanceConfiguration deployConfig) {
        LOGGER.info("Loading state with instance ID: {}", instanceId);
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        SleeperInstanceTables tables = SleeperInstanceTables.load(deployConfig, instanceProperties, s3, dynamoDB, new Configuration());
        return new Instance(identifier, deployConfig, instanceProperties, tables);
    }

    private class Instance {
        private final String identifier;
        private final DeployInstanceConfiguration deployConfiguration;
        private final InstanceProperties instanceProperties;
        private final SleeperInstanceTables tables;
        private GenerateNumberedValueOverrides generatorOverrides = GenerateNumberedValueOverrides.none();

        Instance(String identifier, DeployInstanceConfiguration deployConfiguration,
                 InstanceProperties instanceProperties, SleeperInstanceTables tables) {
            this.identifier = identifier;
            this.deployConfiguration = deployConfiguration;
            this.instanceProperties = instanceProperties;
            this.tables = tables;
        }

        public InstanceProperties getInstanceProperties() {
            return instanceProperties;
        }

        public Stream<Record> generateNumberedRecords(LongStream numbers) {
            return GenerateNumberedRecords.from(tables.getSchema(), generatorOverrides, numbers);
        }

        public void setGeneratorOverrides(GenerateNumberedValueOverrides overrides) {
            this.generatorOverrides = overrides;
        }

        public Instance reload() {
            return loadInstance(
                    identifier,
                    instanceProperties.get(ID),
                    deployConfiguration);
        }

        public Instance redeployIfNeededNoDeployedUpdate() throws InterruptedException {
            boolean redeployNeeded = false;

            Set<String> ingestRoles = new LinkedHashSet<>(instanceProperties.getList(INGEST_SOURCE_ROLE));
            if (systemTest.isSystemTestClusterEnabled() &&
                    !ingestRoles.contains(systemTest.getSystemTestWriterRoleName())) {
                ingestRoles.add(systemTest.getSystemTestWriterRoleName());
                instanceProperties.set(INGEST_SOURCE_ROLE, String.join(",", ingestRoles));
                redeployNeeded = true;
                LOGGER.info("Redeploy required to give system test cluster access to the instance");
            }

            if (!SleeperVersion.getVersion().equals(instanceProperties.get(VERSION))) {
                redeployNeeded = true;
                LOGGER.info("Redeploy required as version number does not match");
            }

            if (isRedeployDueToPropertyChange(UserDefinedInstanceProperty.getAll(),
                    deployConfiguration.getInstanceProperties(), instanceProperties)) {
                redeployNeeded = true;
            }

            if (parameters.isForceRedeployInstances()) {
                LOGGER.info("Forcing redeploy");
                redeployNeeded = true;
            }

            if (redeployNeeded) {
                return redeploy();
            } else {
                return this;
            }
        }

        public Instance redeploy() throws InterruptedException {
            redeployNoReload();
            return reload();
        }

        private void redeployNoReload() throws InterruptedException {
            try {
                DeployExistingInstance.builder()
                        .clients(s3v2, ecr)
                        .properties(instanceProperties)
                        .tableProperties(tables.getTableProperties())
                        .scriptsDirectory(parameters.getScriptsDirectory())
                        .deployCommand(CdkCommand.deployExistingPaused())
                        .runCommand(ClientUtils::runCommandLogOutput)
                        .build().update();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public Instance resetProperties() {
            ResetProperties.reset(instanceProperties, deployConfiguration.getInstanceProperties());
            instanceProperties.saveToS3(s3);
            return new Instance(identifier, deployConfiguration, instanceProperties, tables);
        }

        public Instance resetTables() {
            return new Instance(identifier, deployConfiguration, instanceProperties,
                    tables.reset(instanceProperties, s3, dynamoDB, new Configuration()));
        }

        public Instance deleteTables() {
            return new Instance(identifier, deployConfiguration, instanceProperties,
                    tables.deleteAll(instanceProperties, s3, dynamoDB, new Configuration()));
        }
    }

    private static <P extends SleeperProperty, T extends SleeperProperties<P>> boolean isRedeployDueToPropertyChange(
            List<? extends P> userDefinedProperties, T deployProperties, T foundProperties) {
        boolean redeployNeeded = false;
        for (P property : userDefinedProperties) {
            if (!property.isEditable() || !property.isRunCdkDeployWhenChanged()) {
                // Non-CDK properties get reset before every test in SleeperInstanceContext.resetProperties
                continue;
            }
            if (!deployProperties.isSet(property) || property == TAGS) {
                continue;
            }
            String deployValue = deployProperties.get(property);
            String foundValue = foundProperties.get(property);
            if (!foundProperties.isSet(property) || !Objects.equals(deployValue, foundValue)) {
                foundProperties.set(property, deployValue);
                LOGGER.info("Redeploy required as property changed: {}", property);
                LOGGER.info("Required value: {}", deployValue);
                LOGGER.info("Found value: {}", foundValue);
                redeployNeeded = true;
            }
        }
        return redeployNeeded;
    }

}
