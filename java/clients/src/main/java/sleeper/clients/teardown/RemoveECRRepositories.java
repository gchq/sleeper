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

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.model.DeleteRepositoryRequest;
import com.amazonaws.services.ecr.model.RepositoryNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;

import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_GPU_REPO;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.instance.IngestProperty.ECR_INGEST_REPO;

public class RemoveECRRepositories {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveECRRepositories.class);

    private RemoveECRRepositories() {
    }

    public static void remove(AmazonECR ecr, InstanceProperties properties, List<String> extraRepositories) {
        Stream.concat(Stream.of(ECR_COMPACTION_GPU_REPO, ECR_COMPACTION_REPO, ECR_INGEST_REPO, BULK_IMPORT_REPO, BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO)
                .filter(properties::isSet)
                .map(properties::get),
                extraRepositories.stream())
                .parallel().forEach(repositoryName -> deleteRepository(ecr, repositoryName));
    }

    private static void deleteRepository(AmazonECR ecr, String repositoryName) {
        LOGGER.info("Deleting repository {}", repositoryName);
        try {
            ecr.deleteRepository(new DeleteRepositoryRequest()
                    .withRepositoryName(repositoryName)
                    .withForce(true));
        } catch (RepositoryNotFoundException e) {
            LOGGER.info("Repository not found: {}", repositoryName);
        }
    }
}
