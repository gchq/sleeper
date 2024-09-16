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

package sleeper.clients.deploy;

import com.google.common.io.CharStreams;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.core.CommonTestConstants;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

@Testcontainers
public abstract class JarsBucketITBase {
    @Container
    public static LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse(CommonTestConstants.LOCALSTACK_DOCKER_IMAGE))
            .withServices(LocalStackContainer.Service.S3);

    protected final S3Client s3 = S3Client.builder()
            .endpointOverride(localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    localStackContainer.getAccessKey(), localStackContainer.getSecretKey())))
            .region(Region.of(localStackContainer.getRegion()))
            .build();

    @TempDir
    protected Path tempDir;
    protected final String bucketName = UUID.randomUUID().toString();

    protected boolean uploadJarsToBucket(String bucketName) throws IOException {
        return syncJarsToBucket(bucketName, false);
    }

    protected boolean uploadJarsToBucketDeletingOldJars(String bucketName) throws IOException {
        return syncJarsToBucket(bucketName, true);
    }

    protected boolean syncJarsToBucket(String bucketName, boolean deleteOld) throws IOException {
        return SyncJars.builder()
                .s3(s3)
                .jarsDirectory(tempDir)
                .region(localStackContainer.getRegion())
                .bucketName(bucketName)
                .deleteOldJars(deleteOld)
                .build().sync();
    }

    protected Stream<String> listObjectKeys() {
        return s3.listObjectsV2Paginator(builder -> builder.bucket(bucketName)).stream()
                .flatMap(response -> response.contents().stream())
                .map(S3Object::key);
    }

    protected Instant getObjectLastModified(String key) {
        return s3.headObject(builder -> builder.bucket(bucketName).key(key)).lastModified();
    }

    protected String getObjectContents(String key) {
        return s3.getObject(builder -> builder.bucket(bucketName).key(key),
                (metadata, inputStream) -> CharStreams.toString(new InputStreamReader(inputStream)));
    }
}
