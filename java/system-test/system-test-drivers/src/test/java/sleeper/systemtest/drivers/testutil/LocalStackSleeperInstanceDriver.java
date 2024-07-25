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

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.docker.DeployDockerInstance;
import sleeper.configuration.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SleeperInstanceDriver;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.util.List;

import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.InstanceProperties.getConfigBucketFromInstanceId;

public class LocalStackSleeperInstanceDriver implements SleeperInstanceDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalStackSleeperInstanceDriver.class);

    private final SystemTestParameters parameters;
    private final AmazonS3 s3;
    private final DeployDockerInstance deploy;

    public LocalStackSleeperInstanceDriver(SystemTestParameters parameters, SystemTestClients clients) {
        this.parameters = parameters;
        s3 = clients.getS3();
        deploy = DeployDockerInstance.builder()
                .s3Client(clients.getS3())
                .dynamoDB(clients.getDynamoDB())
                .sqsClient(clients.getSqs())
                .configuration(clients.createHadoopConf())
                .build();
    }

    @Override
    public void loadInstanceProperties(InstanceProperties instanceProperties, String instanceId) {
        LOGGER.info("Loading properties with instance ID: {}", instanceId);
        instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
    }

    @Override
    public void saveInstanceProperties(InstanceProperties instanceProperties) {
        LOGGER.info("Saving properties with instance ID: {}", instanceProperties.get(ID));
        instanceProperties.saveToS3(s3);
    }

    @Override
    public boolean deployInstanceIfNotPresent(String instanceId, DeployInstanceConfiguration deployConfig) {
        if (s3.doesBucketExistV2(getConfigBucketFromInstanceId(instanceId))) {
            return false;
        }
        LOGGER.info("Deploying instance: {}", instanceId);
        InstanceProperties instanceProperties = deployConfig.getInstanceProperties();
        instanceProperties.set(ID, instanceId);
        instanceProperties.set(JARS_BUCKET, parameters.buildJarsBucketName());
        deploy.deploy(instanceProperties, deployConfig.getTableProperties());
        return true;
    }

    @Override
    public void redeploy(InstanceProperties instanceProperties, List<TableProperties> tableProperties) {
        throw new UnsupportedOperationException("Unimplemented method 'redeploy'");
    }

}
