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

package sleeper.clients.teardown;

import org.junit.jupiter.api.Test;

import sleeper.clients.deploy.JarsBucketITBase;

import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static sleeper.clients.util.BucketUtils.doesBucketExist;

class RemoveJarsBucketIT extends JarsBucketITBase {
    @Test
    void shouldRemoveEmptyBucket() throws IOException {
        // Given
        uploadJarsToBucket(bucketName);

        // When
        RemoveJarsBucket.remove(s3, bucketName);

        // Then
        assertThat(doesBucketExist(s3, bucketName)).isFalse();
    }

    @Test
    void shouldRemoveBucketWithJars() throws IOException {
        // Given
        Files.writeString(tempDir.resolve("test.jar"), "data");
        uploadJarsToBucket(bucketName);

        // When
        RemoveJarsBucket.remove(s3, bucketName);

        // Then
        assertThat(doesBucketExist(s3, bucketName)).isFalse();
    }

    @Test
    void shouldRemoveBucketWithMultipleVersions() throws IOException, InterruptedException {
        // Given
        Files.writeString(tempDir.resolve("test.jar"), "data1");
        uploadJarsToBucket(bucketName);
        Thread.sleep(1000);
        Files.writeString(tempDir.resolve("test.jar"), "data2");
        uploadJarsToBucket(bucketName);

        // When
        RemoveJarsBucket.remove(s3, bucketName);

        // Then
        assertThat(doesBucketExist(s3, bucketName)).isFalse();
    }

    @Test
    void shouldIgnoreIfBucketDoesNotExist() {
        assertThatCode(() -> RemoveJarsBucket.remove(s3, "not-a-bucket"))
                .doesNotThrowAnyException();
    }
}
