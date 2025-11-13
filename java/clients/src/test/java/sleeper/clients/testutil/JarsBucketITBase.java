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

package sleeper.clients.testutil;

import com.google.common.io.CharStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.clients.deploy.jar.JarsBucketCreator;
import sleeper.clients.deploy.jar.SyncJars;
import sleeper.clients.deploy.jar.SyncJarsRequest;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

public abstract class JarsBucketITBase extends LocalStackTestBase {

    @TempDir
    protected Path tempDir;
    protected final InstanceProperties instanceProperties = createTestInstanceProperties();
    protected final String bucketName = instanceProperties.get(JARS_BUCKET);

    @BeforeEach
    void setUpBase() {
        new JarsBucketCreator(instanceProperties, s3Client).create();
    }

    protected boolean uploadJarsToBucket(String bucketName) throws IOException {
        return syncJarsToBucket(bucketName, false);
    }

    protected boolean uploadJarsToBucketDeletingOldJars(String bucketName) throws IOException {
        return syncJarsToBucket(bucketName, true);
    }

    protected boolean syncJarsToBucket(String bucketName, boolean deleteOld) throws IOException {
        return new SyncJars(s3Client, tempDir)
                .sync(SyncJarsRequest.builder()
                        .bucketName(bucketName)
                        .region(localStackContainer.getRegion())
                        .deleteOldJars(deleteOld)
                        .build());
    }

    protected Stream<String> listObjectKeys() {
        return s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName)).stream()
                .flatMap(response -> response.contents().stream())
                .map(S3Object::key);
    }

    protected Instant getObjectLastModified(String key) {
        return s3Client.headObject(builder -> builder.bucket(bucketName).key(key)).lastModified();
    }

    protected String getObjectContents(String key) {
        return s3Client.getObject(builder -> builder.bucket(bucketName).key(key),
                (metadata, inputStream) -> CharStreams.toString(new InputStreamReader(inputStream)));
    }
}
