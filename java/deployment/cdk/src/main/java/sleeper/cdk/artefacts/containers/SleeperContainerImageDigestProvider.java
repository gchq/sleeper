/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.artefacts.containers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ecr.model.DescribeImagesResponse;
import software.amazon.awssdk.services.ecr.model.ImageDetail;
import software.amazon.awssdk.services.ecr.model.ImageIdentifier;

import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;

/**
 * Finds container images to deploy. Looks up the latest digest for each image in an ECR repository. The
 * deployment will be done against a specific digest of each image. It will only check the repository once for each
 * image, and you can reuse the same object for multiple Sleeper instances.
 */
public class SleeperContainerImageDigestProvider {

    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperContainerImageDigestProvider.class);

    private final GetDigest getDigest;
    private final Map<String, String> latestDigestByImageName = new HashMap<>();

    public SleeperContainerImageDigestProvider(GetDigest getDigest) {
        this.getDigest = getDigest;
    }

    /**
     * Creates a provider that looks up the latest digest for each image in an ECR repository. The deployment will be
     * done against a specific digest of each image. It will only check the repository once for each image, and you can
     * reuse the same object for multiple Sleeper instances.
     *
     * @param  ecrClient          the ECR client
     * @param  instanceProperties the instance properties
     * @return                    an image digest provider
     */
    public static SleeperContainerImageDigestProvider from(EcrClient ecrClient, InstanceProperties instanceProperties) {
        return new SleeperContainerImageDigestProvider(GetDigest.fromEcrRepository(ecrClient, instanceProperties));
    }

    /**
     * Get the digest of the Docker image.
     *
     * @param  imageName         the name of the image
     * @param  ecrRepositoryName the name of the ECR repository
     * @return                   the digest for the given deployment
     */
    public String getDigestToDeploy(String imageName, String ecrRepositoryName) {
        return latestDigestByImageName.computeIfAbsent(imageName, name -> getDigest.getDigest(imageName, ecrRepositoryName));
    }

    /**
     * Checks the digest for the latest version of a given image in an ECR repository. When we provide the CDK with a
     * specific image digest, it can tell when the image has changed even if it still has the same name.
     */
    public interface GetDigest {
        /**
         * Get the digest of the Docker image.
         *
         * @param  imageName         the name of the image
         * @param  ecrRepositoryName the name of the ECR repository
         * @return                   the digest for the given deployment
         */
        String getDigest(String imageName, String ecrRepositoryName);

        /**
         * Implementation of GetDigest that checks the digest for the latest version of a given image in an ECR
         * repository.
         *
         * @param  ecrClient          the ECR client
         * @param  instanceProperties the instance properties
         * @return                    the get digest implementation
         */
        static GetDigest fromEcrRepository(EcrClient ecrClient, InstanceProperties instanceProperties) {
            return (imageName, ecrRepositoryName) -> {
                LOGGER.info("Checking latest digest for image: {}", imageName);

                DescribeImagesResponse response = ecrClient.describeImages(DescribeImagesRequest.builder()
                        .repositoryName(ecrRepositoryName)
                        .imageIds(ImageIdentifier.builder().imageTag(instanceProperties.get(VERSION)).build())
                        .build());

                String digest = response.imageDetails().stream()
                        .findFirst()
                        .map(ImageDetail::imageDigest)
                        .orElseThrow(() -> new RuntimeException("No image digest found!"));
                LOGGER.info("Found latest digest for image {}: {}", imageName, digest);
                return digest;
            };
        }
    }

}
