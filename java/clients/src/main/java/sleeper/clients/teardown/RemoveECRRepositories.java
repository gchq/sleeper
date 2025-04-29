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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.stream.Stream;

public class RemoveECRRepositories {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveECRRepositories.class);

    private RemoveECRRepositories() {
    }

    public static void remove(EcrClient ecr, InstanceProperties properties, List<String> extraRepositories) {
        remove(ecr, streamAllRepositoryNames(properties, DockerDeployment.all(), LambdaHandler.all(), extraRepositories));
    }

    public static void remove(EcrClient ecr, Stream<String> repositoryNames) {
        repositoryNames.parallel().forEach(repositoryName -> deleteRepository(ecr, repositoryName));
    }

    public static Stream<String> streamAllRepositoryNames(
            InstanceProperties properties, List<DockerDeployment> dockerDeployments, List<LambdaHandler> lambdaHandlers, List<String> extraRepositories) {
        return Stream.of(
                dockerDeployments.stream()
                        .map(deployment -> deployment.getEcrRepositoryName(properties)),
                lambdaHandlers.stream()
                        .map(LambdaHandler::getJar).distinct()
                        .map(jar -> jar.getEcrRepositoryName(properties)),
                extraRepositories.stream())
                .flatMap(names -> names); // Combine all streams (concat only takes 2)
    }

    private static void deleteRepository(EcrClient ecr, String repositoryName) {
        LOGGER.info("Deleting repository {}", repositoryName);
        try {
            ecr.deleteRepository(request -> request
                    .repositoryName(repositoryName)
                    .force(true));
        } catch (RepositoryNotFoundException e) {
            LOGGER.info("Repository not found: {}", repositoryName);
        }
    }
}
