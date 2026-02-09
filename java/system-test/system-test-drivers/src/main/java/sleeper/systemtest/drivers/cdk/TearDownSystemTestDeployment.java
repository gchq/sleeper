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
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

import sleeper.clients.teardown.WaitForStackToDelete;

public class TearDownSystemTestDeployment {
    public static final Logger LOGGER = LoggerFactory.getLogger(TearDownSystemTestDeployment.class);

    private final CloudFormationClient cloudFormationClient;
    private final String deploymentId;

    private TearDownSystemTestDeployment(CloudFormationClient cloudFormationClient, String deploymentId) {
        this.cloudFormationClient = cloudFormationClient;
        this.deploymentId = deploymentId;
    }

    public static TearDownSystemTestDeployment fromDeploymentId(CloudFormationClient cloudFormationClient, String deploymentId) {
        return new TearDownSystemTestDeployment(cloudFormationClient, deploymentId);
    }

    public void deleteStack() {
        deleteStack(deploymentId);
    }

    public void deleteArtefactsStack() {
        deleteStack(artefactsStackName());
    }

    private void deleteStack(String stackName) {
        LOGGER.info("Deleting system test CloudFormation stack: {}", stackName);
        try {
            cloudFormationClient.deleteStack(builder -> builder.stackName(stackName));
        } catch (RuntimeException e) {
            LOGGER.warn("Failed deleting stack", e);
        }
    }

    public void waitForStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(cloudFormationClient, deploymentId).pollUntilFinished();
    }

    public void waitForArtefactsStackToDelete() throws InterruptedException {
        WaitForStackToDelete.from(cloudFormationClient, artefactsStackName()).pollUntilFinished();
    }

    private String artefactsStackName() {
        return deploymentId + "-artefacts";
    }
}
