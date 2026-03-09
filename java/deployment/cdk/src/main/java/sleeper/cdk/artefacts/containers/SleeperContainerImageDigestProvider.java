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
package sleeper.cdk.artefacts.containers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ecr.model.DescribeImagesResponse;
import software.amazon.awssdk.services.ecr.model.ImageIdentifier;

import sleeper.core.deploy.DockerDeployment;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;

/**
 * Finds container images to deploy. Looks up the latest digest for each image in an Ecr repository. The
 * deployment will be done against a specific digest of each image. It will only check the repository once for each
 * image, and
 * you can reuse the same object for multiple Sleeper instances.
 */
public class SleeperContainerImageDigestProvider {

    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperContainerImageDigestProvider.class);

    private final GetDigest getDigest;
    private final Map<DockerDeployment, String> latestDigestByContainer = new HashMap<>();

    public SleeperContainerImageDigestProvider(GetDigest getDigest) {
        this.getDigest = getDigest;
    }

    public static SleeperContainerImageDigestProvider from(EcrClient ecrClient, InstanceProperties instanceProperties) {
        return new SleeperContainerImageDigestProvider(GetDigest.fromEcrRepository(ecrClient, instanceProperties));
    }

    public String getLatestDigest(DockerDeployment deployment) {
        return latestDigestByContainer.computeIfAbsent(deployment, getDigest::getDigest);
    }

    /**
     * Checks the digest for the latest version of a given image in an Ecr repository. When we provide the CDK with a
     * specific image digest, it can tell when the image has changed even if it still has the same name.
     */
    public interface GetDigest {
        String getDigest(DockerDeployment deployment);

        static GetDigest fromEcrRepository(EcrClient ecrClient, InstanceProperties instanceProperties) {
            return deployment -> {
                String image = deployment.getDockerImageName(instanceProperties);
                LOGGER.info("Checking latest digest for image: {}", image);

                DescribeImagesResponse response = ecrClient.describeImages(DescribeImagesRequest.builder()
                        .repositoryName(deployment.getEcrRepositoryName(instanceProperties))
                        .imageIds(ImageIdentifier.builder().imageTag(instanceProperties.get(VERSION)).build())
                        .build());

                String digest = response.imageDetails().get(0).imageDigest();
                LOGGER.info("Found latest digest for image {}: {}", image, digest);
                return digest;
            };
        }
    }

}
