/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.ecr.AmazonECR;
import com.amazonaws.services.ecr.model.CreateRepositoryRequest;
import com.amazonaws.services.ecr.model.DeleteRepositoryRequest;
import com.amazonaws.services.ecr.model.DescribeRepositoriesRequest;
import com.amazonaws.services.ecr.model.ListImagesRequest;
import com.amazonaws.services.ecr.model.ListImagesResult;
import com.amazonaws.services.ecr.model.RepositoryNotFoundException;
import com.amazonaws.services.ecr.model.SetRepositoryPolicyRequest;

import java.util.Objects;

public class EcrRepositoryCreator {
    private EcrRepositoryCreator() {
    }

    public static Client withEcrClient(AmazonECR ecrClient) {
        return new AwsClient(ecrClient);
    }

    private static class AwsClient implements Client {
        private final AmazonECR ecrClient;

        AwsClient(AmazonECR ecrClient) {
            this.ecrClient = ecrClient;
        }

        @Override
        public boolean repositoryExists(String repository) {
            try {
                ecrClient.describeRepositories(new DescribeRepositoriesRequest().withRepositoryNames(repository));
                return true;
            } catch (RepositoryNotFoundException e) {
                return false;
            }
        }

        @Override
        public void createRepository(String repository) {
            ecrClient.createRepository(new CreateRepositoryRequest().withRepositoryName(repository));
        }

        @Override
        public void deleteRepository(String repository) {
            ecrClient.deleteRepository(new DeleteRepositoryRequest().withRepositoryName(repository));
        }

        @Override
        public void createEmrServerlessAccessPolicy(String repository) {
            ecrClient.setRepositoryPolicy(new SetRepositoryPolicyRequest().withRepositoryName(repository)
                    .withPolicyText("" +
                            "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"EmrServerlessCustomImageSupport\"," +
                            "\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"emr-serverless.amazonaws.com\"}," +
                            "\"Action\":[\"ecr:BatchGetImage\",\"ecr:DescribeImages\",\"ecr:GetDownloadUrlForLayer\"]}]}"));
        }

        @Override
        public boolean versionExistsInRepository(String repository, String version) {
            try {
                ListImagesResult result = ecrClient.listImages(new ListImagesRequest().withRepositoryName(repository));
                return result.getImageIds().stream()
                        .anyMatch(imageIdentifier -> Objects.equals(version, imageIdentifier.getImageTag()));
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
