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

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.utility.DockerImageName;

import sleeper.core.CommonTestConstants;
import sleeper.systemtest.dsl.extension.SleeperSystemTestExtension;
import sleeper.systemtest.dsl.instance.SystemTestDeploymentContext;
import sleeper.systemtest.dsl.instance.SystemTestParameters;
import sleeper.systemtest.dsl.testutil.SystemTestParametersTestHelper;

public class LocalStackSystemTestExtension extends SleeperSystemTestExtension {

    private static final LocalStackContainer CONTAINER = startContainer();

    private LocalStackSystemTestExtension() {
        super(setupContext(CONTAINER));
    }

    @SuppressWarnings("resource") // Will be cleaned up by Ryuk
    private static LocalStackContainer startContainer() {
        LocalStackContainer container = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
                .withServices(Service.S3, Service.DYNAMODB, Service.SQS);
        container.start();
        return container;
    }

    private static SystemTestDeploymentContext setupContext(LocalStackContainer container) {
        SystemTestParameters parameters = SystemTestParametersTestHelper.parametersBuilder()
                .region(container.getRegion())
                .build();
        return new SystemTestDeploymentContext(
                parameters, LocalStackSystemTestDrivers.fromContainer(container));
    }

}
