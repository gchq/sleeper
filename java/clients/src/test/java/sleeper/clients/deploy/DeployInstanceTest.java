/*
 * Copyright 2026 Crown Copyright
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
package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import sleeper.clients.deploy.container.UploadDockerImagesToEcr;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.util.cdk.CdkCommand;
import sleeper.clients.util.cdk.InvokeCdk;
import sleeper.core.deploy.SleeperInstanceConfiguration;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.SleeperInternalCdkApp;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static sleeper.core.properties.instance.CommonProperty.ARTEFACTS_DEPLOYMENT_ID;

class DeployInstanceTest {

    @Test
    void shouldReportConfiguredArtefactsDeploymentWhenJarsBucketDoesNotExist() throws Exception {
        // Given
        SyncJars syncJars = mock(SyncJars.class);
        doThrow(NoSuchBucketException.builder().message("The specified bucket does not exist").build())
                .when(syncJars).sync(any());
        DeployInstance deployer = new DeployInstance(syncJars, mock(UploadDockerImagesToEcr.class), mock(InvokeCdk.class));
        InstanceProperties properties = new InstanceProperties();
        properties.set(ARTEFACTS_DEPLOYMENT_ID, "missing-artefacts");
        DeployInstanceRequest request = DeployInstanceRequest.builder()
                .instanceConfig(SleeperInstanceConfiguration.withNoTables(properties))
                .cdkCommand(CdkCommand.deployNew())
                .cdkApp(SleeperInternalCdkApp.STANDARD)
                .build();

        // When / Then
        assertThatThrownBy(() -> deployer.deploy(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Artefacts deployment does not exist: missing-artefacts")
                .hasCauseInstanceOf(NoSuchBucketException.class);
    }
}
