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

package sleeper.clients.teardown;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException;

import sleeper.core.deploy.LambdaHandler;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.validation.LambdaDeployType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.ECR_REPOSITORY_PREFIX;
import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.LAMBDA_DEPLOY_TYPE;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_GPU_REPO;
import static sleeper.core.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.core.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.core.properties.instance.IngestProperty.ECR_INGEST_REPO;

public class RemoveECRRepositories {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveECRRepositories.class);

    private RemoveECRRepositories() {
    }

    public static void remove(EcrClient ecr, InstanceProperties properties, List<String> extraRepositories) {
        Stream.of(repositoryNamesFromProperties(properties),
                lambdaRepositoryNames(properties),
                extraRepositories.stream())
                .flatMap(s -> s)
                .parallel().forEach(repositoryName -> deleteRepository(ecr, repositoryName));
    }

    private static Stream<String> repositoryNamesFromProperties(InstanceProperties properties) {
        return Stream.of(ECR_COMPACTION_GPU_REPO, ECR_COMPACTION_REPO, ECR_INGEST_REPO, BULK_IMPORT_REPO, BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO)
                .filter(properties::isSet)
                .map(properties::get);
    }

    private static Stream<String> lambdaRepositoryNames(InstanceProperties properties) {
        if (properties.getEnumValue(LAMBDA_DEPLOY_TYPE, LambdaDeployType.class) == LambdaDeployType.CONTAINER) {
            String ecrPrefix = Optional.ofNullable(properties.get(ECR_REPOSITORY_PREFIX)).orElseGet(() -> properties.get(ID));
            return LambdaHandler.all().stream()
                    .map(LambdaHandler::getJar).distinct()
                    .map(jar -> ecrPrefix + "/" + jar.getImageName());
        } else {
            return Stream.of();
        }
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
