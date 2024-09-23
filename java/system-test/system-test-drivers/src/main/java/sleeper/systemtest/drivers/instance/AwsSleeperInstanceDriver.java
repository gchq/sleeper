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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.cloudformation.model.Stack;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.deploy.DeployExistingInstance;
import sleeper.clients.deploy.DeployNewInstance;
import sleeper.clients.util.ClientUtils;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static software.amazon.awssdk.services.cloudformation.model.StackStatus.CREATE_FAILED;
import static software.amazon.awssdk.services.cloudformation.model.StackStatus.ROLLBACK_COMPLETE;

public class AwsSleeperInstanceDriver implements SleeperInstanceDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsSleeperInstanceDriver.class);

    private final SystemTestParameters parameters;
    private final AmazonS3 s3;
    private final S3Client s3v2;
    private final AmazonDynamoDB dynamoDB;
    private final AWSSecurityTokenService sts;
    private final AwsRegionProvider regionProvider;
    private final CloudFormationClient cloudFormationClient;
    private final EcrClient ecr;

    public AwsSleeperInstanceDriver(SystemTestParameters parameters, SystemTestClients clients) {
        this.parameters = parameters;
        this.s3 = clients.getS3();
        this.s3v2 = clients.getS3V2();
        this.dynamoDB = clients.getDynamoDB();
        this.sts = clients.getSts();
        this.regionProvider = clients.getRegionProvider();
        this.cloudFormationClient = clients.getCloudFormation();
        this.ecr = clients.getEcr();
    }

    public void loadInstanceProperties(InstanceProperties instanceProperties, String instanceId) {
        LOGGER.info("Loading properties with instance ID: {}", instanceId);
        S3InstanceProperties.reloadGivenInstanceId(s3, instanceProperties, instanceId);
    }

    public void saveInstanceProperties(InstanceProperties instanceProperties) {
        instanceProperties.saveToS3(s3);
    }

    public boolean deployInstanceIfNotPresent(String instanceId, DeployInstanceConfiguration deployConfig) {
        if (deployedStackIsPresent(instanceId)) {
            return false;
        }
        LOGGER.info("Deploying instance: {}", instanceId);
        try {
            DeployNewInstance.builder().scriptsDirectory(parameters.getScriptsDirectory())
                    .deployInstanceConfiguration(deployConfig)
                    .instanceId(instanceId)
                    .vpcId(parameters.getVpcId())
                    .subnetIds(parameters.getSubnetIds())
                    .deployPaused(true)
                    .instanceType(InvokeCdkForInstance.Type.STANDARD)
                    .runCommand(ClientUtils::runCommandLogOutput)
                    .extraInstanceProperties(instanceProperties -> instanceProperties.set(JARS_BUCKET, parameters.buildJarsBucketName()))
                    .deployWithClients(sts, regionProvider, s3, s3v2, dynamoDB, ecr);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    private boolean deployedStackIsPresent(String instanceId) {
        try {
            Stack stack = cloudFormationClient.describeStacks(builder -> builder.stackName(instanceId))
                    .stacks().stream().findAny().orElseThrow();
            LOGGER.info("Stack already exists: {}", stack);
            if (Set.of(CREATE_FAILED, ROLLBACK_COMPLETE).contains(stack.stackStatus())) {
                LOGGER.warn("Stack in invalid state: {}", stack.stackStatus());
                return false;
            } else {
                return true;
            }
        } catch (CloudFormationException e) {
            return false;
        }
    }

    public void redeploy(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        try {
            DeployExistingInstance.builder()
                    .clients(s3v2, ecr)
                    .properties(instanceProperties)
                    .tablePropertiesList(tableProperties)
                    .scriptsDirectory(parameters.getScriptsDirectory())
                    .deployCommand(CdkCommand.deployExistingPaused())
                    .runCommand(ClientUtils::runCommandLogOutput)
                    .build().update();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        loadInstanceProperties(instanceProperties, instanceProperties.get(ID));
    }
}
