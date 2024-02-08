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
import sleeper.core.util.PollWithRetries;

import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class WaitForS3BucketToEmptyIT extends JarsBucketITBase {
    @Test
    void shouldTimeOutIfObjectsAreStillInBucket() throws Exception {
        // Given
        Files.writeString(tempDir.resolve("test.jar"), "data1");
        uploadJarsToBucket(bucketName);

        // When
        WaitForS3BucketToEmpty waitForS3BucketToEmpty = new WaitForS3BucketToEmpty(
                s3, bucketName, PollWithRetries.noRetries());

        // Then
        assertThatThrownBy(waitForS3BucketToEmpty::pollUntilFinished)
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    @Test
    void shouldFinishWaitingIfNoObjectsAreInBucket() throws Exception {
        // Given
        uploadJarsToBucket(bucketName);

        // When
        WaitForS3BucketToEmpty waitForS3BucketToEmpty = WaitForS3BucketToEmpty.from(s3, bucketName);

        // Then
        assertThatCode(waitForS3BucketToEmpty::pollUntilFinished)
                .doesNotThrowAnyException();
    }
}
