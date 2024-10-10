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
package sleeper.systemtest.drivers.testutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.core.SleeperVersion;
import sleeper.core.properties.deploy.DeployInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.List;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

public class LocalStackSleeperInstanceDriver implements SleeperInstanceDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStackSleeperInstanceDriver.class);

    private final SystemTestParameters parameters;
    private final SystemTestClients clients;

    public LocalStackSleeperInstanceDriver(SystemTestParameters parameters, SystemTestClients clients) {
        this.parameters = parameters;
        this.clients = clients;
    }

    @Override
    public void loadInstanceProperties(InstanceProperties instanceProperties, String instanceId) {
        LOGGER.info("Loading properties with instance ID: {}", instanceId);
        S3InstanceProperties.reloadGivenInstanceId(clients.getS3(), instanceProperties, instanceId);
    }

    @Override
    public void saveInstanceProperties(InstanceProperties instanceProperties) {
        LOGGER.info("Saving properties with instance ID: {}", instanceProperties.get(ID));
        S3InstanceProperties.saveToS3(clients.getS3(), instanceProperties);
    }

    @Override
    public boolean deployInstanceIfNotPresent(String instanceId, DeployInstanceConfiguration deployConfig) {
        if (clients.getS3().doesBucketExistV2(InstanceProperties.getConfigBucketFromInstanceId(instanceId))) {
            return false;
        }
        LOGGER.info("Deploying instance: {}", instanceId);
        InstanceProperties instanceProperties = deployConfig.getInstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(JARS_BUCKET, parameters.buildJarsBucketName());
        instanceProperties.set(VERSION, SleeperVersion.getVersion());
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createStateStoreCommitterQueue(instanceId).queueUrl());
        DeployDockerInstance.builder()
                .s3Client(clients.getS3())
                .dynamoDB(clients.getDynamoDB())
                .sqsClient(clients.getSqsV2())
                .configuration(clients.createHadoopConf())
                .build().deploy(instanceProperties, deployConfig.getTableProperties());
        return true;
    }

    private CreateQueueResponse createStateStoreCommitterQueue(String instanceId) {
        return clients.getSqsV2().createQueue(request -> request
                .queueName(String.join("-", "sleeper", instanceId, "StateStoreCommitterQ.fifo"))
                .attributes(Map.of(
                        QueueAttributeName.FIFO_QUEUE, "true",
                        QueueAttributeName.FIFO_THROUGHPUT_LIMIT, "perMessageGroupId",
                        QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup")));
    }

    @Override
    public void redeploy(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        throw new UnsupportedOperationException("Unimplemented method 'redeploy'");
    }

}
