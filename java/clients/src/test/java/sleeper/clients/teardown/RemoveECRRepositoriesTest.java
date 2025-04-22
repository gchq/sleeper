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
package sleeper.clients.teardown;

import org.junit.jupiter.api.Test;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.OptionalStack;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public class RemoveECRRepositoriesTest {

    private static final List<DockerDeployment> DOCKER_DEPLOYMENTS = List.of(
            DockerDeployment.builder()
                    .deploymentName("ingest")
                    .optionalStack(OptionalStack.IngestStack)
                    .build(),
            DockerDeployment.builder()
                    .deploymentName("compaction")
                    .optionalStack(OptionalStack.CompactionStack)
                    .multiplatform(true)
                    .build());
    private static final LambdaJar STATESTORE_JAR = LambdaJar.withFormatAndImage("statestore.jar", "statestore-lambda");
    private static final List<LambdaHandler> LAMBDA_HANDLERS = List.of(
            LambdaHandler.builder().jar(STATESTORE_JAR)
                    .handler("StateStoreCommitterLambda").core().build(),
            LambdaHandler.builder().jar(STATESTORE_JAR)
                    .handler("SnapshotCreationLambda").core().build(),
            LambdaHandler.builder().jar(LambdaJar.withFormatAndImage("ingest.jar", "ingest-task-creator-lambda"))
                    .handler("IngestTaskCreatorLambda")
                    .optionalStack(OptionalStack.IngestStack).build());

    private final InstanceProperties instanceProperties = createTestInstanceProperties();

    @Test
    void shouldGetRepositoryNamesForAllImages() {
        instanceProperties.set(ID, "test-instance");
        assertThat(RemoveECRRepositories.streamAllRepositoryNames(instanceProperties, DOCKER_DEPLOYMENTS, LAMBDA_HANDLERS))
                .containsExactly(
                        "test-instance/ingest", "test-instance/compaction",
                        "test-instance/statestore-lambda", "test-instance/ingest-task-creator-lambda");
    }

}
