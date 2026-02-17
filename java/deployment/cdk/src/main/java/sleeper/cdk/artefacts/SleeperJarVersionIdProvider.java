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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.deploy.LambdaJar;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.VERSION;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;

/**
 * Finds jars to deploy lambda functions. Looks up the latest version of each jar in a versioned S3 bucket. The
 * deployment will be done against a specific version of each jar. It will only check the bucket once for each jar, and
 * you can reuse the same object for multiple Sleeper instances.
 */
public class SleeperJarVersionIdProvider {

    public static final Logger LOGGER = LoggerFactory.getLogger(SleeperJarVersionIdProvider.class);

    private final GetVersionId getVersionId;
    private final Map<LambdaJar, String> latestVersionIdByJar = new HashMap<>();

    public SleeperJarVersionIdProvider(GetVersionId getVersionId) {
        this.getVersionId = getVersionId;
    }

    public static SleeperJarVersionIdProvider from(S3Client s3, InstanceProperties instanceProperties) {
        return new SleeperJarVersionIdProvider(GetVersionId.fromJarsBucket(s3, instanceProperties));
    }

    public String getLatestVersionId(LambdaJar jar) {
        return latestVersionIdByJar.computeIfAbsent(jar, getVersionId::getVersionId);
    }

    /**
     * Checks the version ID of the latest version of a given jar in the jars bucket. When we provide the CDK with a
     * specific S3 version ID, it can tell when the jar has changed even if it still has the same filename.
     */
    public interface GetVersionId {
        String getVersionId(LambdaJar jar);

        static GetVersionId fromJarsBucket(S3Client s3Client, InstanceProperties instanceProperties) {
            return jar -> {
                String filename = jar.getFilename(instanceProperties.get(VERSION));
                String bucketName = instanceProperties.get(JARS_BUCKET);
                LOGGER.info("Checking version ID for jar: {}", filename);
                String versionId = s3Client.headObject(builder -> builder.bucket(bucketName).key(filename)).versionId();
                LOGGER.info("Found latest version ID for jar {}: {}", filename, versionId);
                return versionId;
            };
        }
    }
}
