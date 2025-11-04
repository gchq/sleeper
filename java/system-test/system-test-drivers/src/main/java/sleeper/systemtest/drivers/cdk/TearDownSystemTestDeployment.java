/*
 * Copyright 2022-2025 Crown Copyright
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.clients.teardown.TearDownClients;
import sleeper.clients.teardown.WaitForStackToDelete;
import sleeper.systemtest.configuration.SystemTestStandaloneProperties;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_ID;

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
        deleteStack(deploymentId);
        deleteStack(deploymentId + "-artefacts");
    }

    private void deleteStack(String stackName) {
        LOGGER.info("Deleting system test CloudFormation stack: {}", stackName);
        try {
            clients.getCloudFormation().deleteStack(builder -> builder.stackName(stackName));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }
    }

    public void waitForStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(clients.getCloudFormation(), properties.get(SYSTEM_TEST_ID)).pollUntilFinished();
    }

    private static SystemTestStandaloneProperties loadOrDefaultProperties(S3Client s3, String deploymentId) {
        try {
            return SystemTestStandaloneProperties.fromS3GivenDeploymentId(s3, deploymentId);
        } catch (RuntimeException e) {
            LOGGER.warn("Could not load system test properties: {}", e.getMessage());
            SystemTestStandaloneProperties properties = new SystemTestStandaloneProperties();
            properties.set(SYSTEM_TEST_ID, deploymentId);
            return properties;
        }
    }
}
