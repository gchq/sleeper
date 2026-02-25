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
package sleeper.cdk.artefacts;

import software.amazon.awssdk.services.s3.S3Client;

import sleeper.cdk.artefacts.containers.SleeperContainerImages;
import sleeper.cdk.artefacts.containers.SleeperContainerImagesFromProperties;
import sleeper.cdk.artefacts.jars.SleeperJarVersionIdProvider;
import sleeper.cdk.artefacts.jars.SleeperJars;
import sleeper.cdk.artefacts.jars.SleeperJarsFromProperties;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * Points the CDK to deployment artefacts in AWS. This will include jars in the jars bucket, and Docker images in AWS
 * ECR.
 */
public interface SleeperArtefacts {

    /**
     * Creates a reference to the artefacts for deploying a specific Sleeper instance.
     *
     * @param  instanceProperties the instance properties
     * @return                    the artefacts
     */
    SleeperInstanceArtefacts forInstance(InstanceProperties instanceProperties);

    /**
     * Retrieves existing artefacts from AWS based on settings in the instance properties. Takes container images from
     * AWS ECR and jars from S3. Requires an S3 client to look up the version ID of each jar in the S3 bucket.
     *
     * @param  s3Client the S3 client
     * @return          the artefacts for deployment of Sleeper
     */
    static SleeperArtefacts fromProperties(S3Client s3Client) {
        return instanceProperties -> new SleeperInstanceArtefacts(instanceProperties,
                new SleeperJarsFromProperties(instanceProperties,
                        SleeperJarVersionIdProvider.from(s3Client, instanceProperties)),
                new SleeperContainerImagesFromProperties(instanceProperties));
    }

    /**
     * Retrieves existing artefacts from AWS based on settings in the instance properties. Takes container images from
     * AWS ECR and jars from S3. Requires a provider to look up the version ID of each jar in the S3 bucket.
     *
     * @param  versionIdProvider the provider to look up the jar version IDs
     * @return                   the artefacts for deployment of Sleeper
     */
    static SleeperArtefacts fromProperties(SleeperJarVersionIdProvider versionIdProvider) {
        return instanceProperties -> new SleeperInstanceArtefacts(instanceProperties,
                new SleeperJarsFromProperties(instanceProperties, versionIdProvider),
                new SleeperContainerImagesFromProperties(instanceProperties));
    }

    /**
     * Points to artefacts by some custom method.
     *
     * @param  jars   the reference to jars to be deployed
     * @param  images the reference to container images to be deployed
     * @return        the artefacts for deployment of Sleeper
     */
    static SleeperArtefacts from(SleeperJars jars, SleeperContainerImages images) {
        return instanceProperties -> new SleeperInstanceArtefacts(instanceProperties, jars, images);
    }

}
