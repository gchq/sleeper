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
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.localstack.test.LocalStackTestBase;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class JarsBucketITBase extends LocalStackTestBase {

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
                .s3(S3_CLIENT_V2)
                .jarsDirectory(tempDir)
                .region(localStackContainer.getRegion())
                .bucketName(bucketName)
                .deleteOldJars(deleteOld)
                .build().sync();
    }

    protected Stream<String> listObjectKeys() {
        return S3_CLIENT_V2.listObjectsV2Paginator(builder -> builder.bucket(bucketName)).stream()
                .flatMap(response -> response.contents().stream())
                .map(S3Object::key);
    }

    protected Instant getObjectLastModified(String key) {
        return S3_CLIENT_V2.headObject(builder -> builder.bucket(bucketName).key(key)).lastModified();
    }

    protected String getObjectContents(String key) {
        return S3_CLIENT_V2.getObject(builder -> builder.bucket(bucketName).key(key),
                (metadata, inputStream) -> CharStreams.toString(new InputStreamReader(inputStream)));
    }
}
