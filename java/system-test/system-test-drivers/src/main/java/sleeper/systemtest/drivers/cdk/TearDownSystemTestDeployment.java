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
package sleeper.systemtest.drivers.cdk;

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.deploy.PopulateInstanceProperties;
import sleeper.clients.teardown.RemoveECRRepositories;
import sleeper.clients.teardown.RemoveJarsBucket;
import sleeper.clients.teardown.ShutdownSystemProcesses;
import sleeper.clients.teardown.TearDownClients;
import sleeper.clients.teardown.WaitForStackToDelete;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.io.IOException;
import java.util.List;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_CLUSTER_NAME;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_JARS_BUCKET;
import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_REPO;

public class TearDownSystemTestDeployment {
    public static final Logger LOGGER = LoggerFactory.getLogger(TearDownSystemTestDeployment.class);

    private final TearDownClients clients;
    private final SystemTestStandaloneProperties properties;

    private TearDownSystemTestDeployment(TearDownClients clients, SystemTestStandaloneProperties properties) {
        this.clients = clients;
        this.properties = properties;
    }

    public static TearDownSystemTestDeployment fromDeploymentId(TearDownClients clients, String deploymentId) {
        return new TearDownSystemTestDeployment(clients, loadOrDefaultProperties(clients.getS3(), deploymentId));
    }

    public void deleteStack() {
        String deploymentId = properties.get(SYSTEM_TEST_ID);
        LOGGER.info("Deleting system test CloudFormation stack: {}", deploymentId);
        try {
            clients.getCloudFormation().deleteStack(builder -> builder.stackName(deploymentId));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }
    }

    public void waitForStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(clients.getCloudFormation(), properties.get(SYSTEM_TEST_ID)).pollUntilFinished();
    }

    public void shutdownSystemProcesses() throws InterruptedException {
        ShutdownSystemProcesses.stopTasks(clients.getEcs(), properties, SYSTEM_TEST_CLUSTER_NAME);
    }

    public void cleanupAfterAllInstancesAndStackDeleted() throws InterruptedException, IOException {
        LOGGER.info("Removing the Jars bucket and docker containers");
        RemoveJarsBucket.remove(clients.getS3v2(), properties.get(SYSTEM_TEST_JARS_BUCKET));
        RemoveECRRepositories.remove(clients.getEcr(),
                PopulateInstanceProperties.generateTearDownDefaultsFromInstanceId(properties.get(SYSTEM_TEST_ID)),
                List.of(properties.get(SYSTEM_TEST_REPO)));
    }

    private static SystemTestStandaloneProperties loadOrDefaultProperties(AmazonS3 s3, String deploymentId) {
        try {
            return SystemTestStandaloneProperties.fromS3GivenDeploymentId(s3, deploymentId);
        } catch (RuntimeException e) {
            LOGGER.warn("Could not load system test properties: {}", e.getMessage());
            SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties();
            properties.set(SYSTEM_TEST_ID, deploymentId);
            properties.set(SYSTEM_TEST_JARS_BUCKET, SystemTestParameters.buildJarsBucketName(deploymentId));
            properties.set(SYSTEM_TEST_REPO, SystemTestParameters.buildSystemTestECRRepoName(deploymentId));
            return properties;
        }
    }
}
