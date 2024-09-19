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

package sleeper.clients.util;

import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.ListImagesResponse;
import software.amazon.awssdk.services.ecr.model.RepositoryNotFoundException;

import java.util.Objects;

public class EcrRepositoryCreator {
    private EcrRepositoryCreator() {
    }

    public static Client withEcrClient(EcrClient ecrClient) {
        return new AwsClient(ecrClient);
    }

    private static class AwsClient implements Client {
        private final EcrClient ecrClient;

        AwsClient(EcrClient ecrClient) {
            this.ecrClient = ecrClient;
        }

        @Override
        public boolean repositoryExists(String repository) {
            try {
                ecrClient.describeRepositories(request -> request.repositoryNames(repository));
                return true;
            } catch (RepositoryNotFoundException e) {
                return false;
            }
        }

        @Override
        public void createRepository(String repository) {
            ecrClient.createRepository(request -> request.repositoryName(repository));
        }

        @Override
        public void deleteRepository(String repository) {
            ecrClient.deleteRepository(request -> request.repositoryName(repository));
        }

        @Override
        public void createEmrServerlessAccessPolicy(String repository) {
            ecrClient.setRepositoryPolicy(request -> request
                    .repositoryName(repository)
                    .policyText("" +
                            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"EmrServerlessCustomImageSupport\"," +
                            "\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"emr-serverless.amazonaws.com\"}," +
                            "\"Action\":[\"ecr:BatchGetImage\",\"ecr:DescribeImages\",\"ecr:GetDownloadUrlForLayer\"]}]}"));
        }

        @Override
        public boolean versionExistsInRepository(String repository, String version) {
            try {
                ListImagesResponse response = ecrClient.listImages(request -> request.repositoryName(repository));
                return response.imageIds().stream()
                        // Note that image tag can be null. In particular, a multiplatform image has multiple images but
                        // only the image index is tagged.
                        .anyMatch(imageIdentifier -> Objects.equals(version, imageIdentifier.imageTag()));
            } catch (RepositoryNotFoundException e) {
                return false;
            }
        }
    }

    public interface Client {
        boolean repositoryExists(String repository);

        void createRepository(String repository);

        void deleteRepository(String repository);

        void createEmrServerlessAccessPolicy(String repository);

        boolean versionExistsInRepository(String repository, String version);
    }
}
