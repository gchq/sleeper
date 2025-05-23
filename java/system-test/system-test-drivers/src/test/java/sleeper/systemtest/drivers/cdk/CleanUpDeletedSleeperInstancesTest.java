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

import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.container.DockerImageConfiguration;
import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.model.OptionalStack;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.systemtest.drivers.cdk.CleanUpDeletedSleeperInstances.instanceIdsByEcrRepositories;
import static sleeper.systemtest.drivers.cdk.CleanUpDeletedSleeperInstances.instanceIdsByJarsBuckets;

public class CleanUpDeletedSleeperInstancesTest {

    @Test
    void shouldGetInstanceIdsFromJarsBuckets() {
        assertThat(instanceIdsByJarsBuckets(
                Stream.of("sleeper-an-instance-jars",
                        "not-a-jars-bucket",
                        "not-sleeper-jars",
                        "sleeper-another-instance-jars",
                        "sleeper-instance-config")))
                .containsExactly("an-instance", "another-instance");
    }

    @Test
    void shouldGetInstanceIdsFromEcrRepositories() {
        assertThat(instanceIdsByEcrRepositories(
                new DockerImageConfiguration(List.of(
                        DockerDeployment.builder()
                                .deploymentName("ingest")
                                .optionalStack(OptionalStack.IngestStack)
                                .build(),
                        DockerDeployment.builder()
                                .deploymentName("compaction-job-execution")
                                .optionalStack(OptionalStack.CompactionStack)
                                .multiplatform(true)
                                .build()),
                        List.of()),
                Stream.of("an-instance/ingest",
                        "not-sleeper/something",
                        "not-an-instance",
                        "another-instance/compaction-job-execution")))
                .containsExactly("an-instance", "another-instance");
    }
}
