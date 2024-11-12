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

import software.amazon.awssdk.services.ecr.model.Repository;
import software.amazon.awssdk.services.s3.model.Bucket;

import sleeper.clients.deploy.DockerImageConfiguration;
import sleeper.clients.teardown.CloudFormationStacks;
import sleeper.clients.teardown.TearDownClients;
import sleeper.clients.teardown.TearDownInstance;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.systemtest.dsl.instance.SystemTestParameters.buildSystemTestECRRepoName;

/**
 * This class can be used to clean up after deleted Sleeper instances when the CloudFormation stacks for those instances
 * have been deleted. This is intended to delete old jars buckets and EMR repositories.
 */
public class CleanUpDeletedSleeperInstances {

    private final TearDownClients clients;
    private final TearDownInstance.Builder tearDownBuilder;
    private final DockerImageConfiguration dockerImageConfiguration = DockerImageConfiguration.getDefault();

    public CleanUpDeletedSleeperInstances(TearDownClients clients, TearDownInstance.Builder tearDownBuilder) {
        this.clients = clients;
        this.tearDownBuilder = tearDownBuilder;
    }

    public void run() throws IOException, InterruptedException {
        for (String instanceId : getInstanceIds()) {
            tearDownBuilder.instanceId(instanceId)
                    .getExtraEcrRepositories(properties -> List.of(buildSystemTestECRRepoName(instanceId)))
                    .build().tearDown();
        }
    }

    private Iterable<String> getInstanceIds() {
        CloudFormationStacks stacks = new CloudFormationStacks(clients.getCloudFormation());
        return () -> Stream.concat(
                instanceIdsByJarsBuckets(),
                instanceIdsByEcrRepositories())
                .filter(instanceId -> !stacks.getStackNames().contains(instanceId))
                .distinct().iterator();
    }

    private Stream<String> instanceIdsByJarsBuckets() {
        return instanceIdsByJarsBuckets(clients.getS3v2().listBuckets().buckets().stream().map(Bucket::name));
    }

    private Stream<String> instanceIdsByEcrRepositories() {
        return instanceIdsByEcrRepositories(dockerImageConfiguration, allRepositoryNames());
    }

    private Stream<String> allRepositoryNames() {
        return clients.getEcr().describeRepositoriesPaginator().stream()
                .flatMap(result -> result.repositories().stream())
                .map(Repository::repositoryName);
    }

    public static Stream<String> instanceIdsByJarsBuckets(Stream<String> bucketNames) {
        return bucketNames
                .filter(bucket -> bucket.startsWith("sleeper-") && bucket.endsWith("-jars"))
                .map(bucket -> bucket.substring("sleeper-".length(), bucket.length() - "-jars".length()));
    }

    public static Stream<String> instanceIdsByEcrRepositories(
            DockerImageConfiguration dockerImageConfiguration,
            Stream<String> repositoryNames) {
        return repositoryNames
                .flatMap(repo -> dockerImageConfiguration.getInstanceIdFromRepoName(repo).stream());
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <scripts directory>");
        }
        Path scriptsDir = Paths.get(args[0]);
        TearDownClients.withDefaults(clients -> new CleanUpDeletedSleeperInstances(clients,
                TearDownInstance.builder().clients(clients).scriptsDir(scriptsDir))
                .run());
    }
}
