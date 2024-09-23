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

import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.configuration.properties.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.S3InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.SleeperVersion;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.List;
import java.util.Map;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.STATESTORE_COMMITTER_QUEUE_URL;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;

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
        instanceProperties.saveToS3(clients.getS3());
    }

    @Override
    public boolean deployInstanceIfNotPresent(String instanceId, DeployInstanceConfiguration deployConfig) {
        if (clients.getS3().doesBucketExistV2(S3InstanceProperties.getConfigBucketFromInstanceId(instanceId))) {
            return false;
        }
        LOGGER.info("Deploying instance: {}", instanceId);
        InstanceProperties instanceProperties = deployConfig.getInstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(JARS_BUCKET, parameters.buildJarsBucketName());
        instanceProperties.set(VERSION, SleeperVersion.getVersion());
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createStateStoreCommitterQueue(instanceId).getQueueUrl());
        DeployDockerInstance.builder()
                .s3Client(clients.getS3())
                .dynamoDB(clients.getDynamoDB())
                .sqsClient(clients.getSqs())
                .configuration(clients.createHadoopConf())
                .build().deploy(instanceProperties, deployConfig.getTableProperties());
        return true;
    }

    private CreateQueueResult createStateStoreCommitterQueue(String instanceId) {
        return clients.getSqs().createQueue(new CreateQueueRequest()
                .withQueueName(String.join("-", "sleeper", instanceId, "StateStoreCommitterQ.fifo"))
                .withAttributes(Map.of("FifoQueue", "true",
                        "FifoThroughputLimit", "perMessageGroupId",
                        "DeduplicationScope", "messageGroup")));
    }

    @Override
    public void redeploy(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        throw new UnsupportedOperationException("Unimplemented method 'redeploy'");
    }

}
