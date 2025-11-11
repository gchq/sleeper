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
package sleeper.clients.deploy.container;

import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.ListImagesResponse;
import software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException;

import java.util.Objects;

public interface CheckVersionExistsInEcr {

    boolean versionExistsInRepository(String repository, String version);

    static CheckVersionExistsInEcr withEcrClient(EcrClient ecrClient) {
        return (repository, version) -> {
            try {
                ListImagesResponse response = ecrClient.listImages(request -> request.repositoryName(repository));
                // Note that the image tag can be null. In particular, a multiplatform image has multiple images but
                // only the image index is tagged.
                return response.imageIds().stream()
                        .anyMatch(imageIdentifier -> Objects.equals(version, imageIdentifier.imageTag()));
            } catch (RepositoryNotFoundException e) {
                return false;
            }
        };
    }

}
