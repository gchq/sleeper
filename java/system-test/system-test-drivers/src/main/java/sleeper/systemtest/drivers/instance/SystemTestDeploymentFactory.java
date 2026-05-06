/*
 * Copyright 2022-2026 Crown Copyright
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

import sleeper.clients.deploy.DeployConfiguration;
import sleeper.clients.deploy.DeployInstance;
import sleeper.clients.deploy.container.UploadDockerImages;
import sleeper.clients.deploy.container.UploadDockerImages.CopyContainerImage;
import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.instance.SystemTestParameters;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SystemTestDeploymentFactory {

    private SystemTestDeploymentFactory() {
    }

    public static DeployInstance createDeployInstance(SystemTestParameters parameters, SystemTestClients clients) {
        return new DeployInstance(
                createSyncJars(parameters, clients),
                createDockerUploader(parameters, clients),
                DeployInstance.WriteLocalProperties.underScriptsDirectory(parameters.getScriptsDirectory()),
                createInvokeCdk(parameters, clients));
    }

    public static SyncJars createSyncJars(SystemTestParameters parameters, SystemTestClients clients) {
        return SyncJars.fromScriptsDirectory(clients.getS3(), parameters.getAccount(), parameters.getScriptsDirectory());
    }

    public static UploadDockerImagesToEcr createDockerUploader(SystemTestParameters parameters, SystemTestClients clients) {
        try {
            return new UploadDockerImagesToEcr(
                    UploadDockerImages.builder()
                            .scriptsDirectory(parameters.getScriptsDirectory())
                            .deployConfig(DeployConfiguration.fromScriptsDirectory(parameters.getScriptsDirectory()))
                            .commandRunner(clients.getCommandRunner())
                            .copyImage(CopyContainerImage.withTransferManager(clients.getEcr()))
                            .build(),
                    parameters.getAccount(), parameters.getRegion());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static InvokeCdk createInvokeCdk(SystemTestParameters parameters, SystemTestClients clients) {
        return InvokeCdk.builder()
                .scriptsDirectory(parameters.getScriptsDirectory())
                .runCommand(clients.getCommandRunner())
                .build();
    }

}
