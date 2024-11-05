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

import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.DockerImageConfiguration;
import sleeper.core.properties.validation.OptionalStack;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildImage;
import static sleeper.clients.deploy.StackDockerImage.dockerBuildxImage;
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
                new DockerImageConfiguration(Map.of(
                        OptionalStack.IngestStack, List.of(dockerBuildImage("ingest")),
                        OptionalStack.CompactionStack, List.of(dockerBuildxImage("compaction-job-execution"))),
                        List.of()),
                Stream.of("an-instance/ingest",
                        "not-sleeper/something",
                        "not-an-instance",
                        "another-instance/compaction-job-execution")))
                .containsExactly("an-instance", "another-instance");
    }
}
