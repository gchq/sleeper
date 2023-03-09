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

package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.util.BucketUtils.doesBucketExist;

public class RemoveJarsBucketIT extends JarsBucketITBase {
    @Test
    void shouldRemoveEmptyBucket() throws IOException {
        // Given
        uploadJarsToBucket(bucketName);

        // When
        RemoveJarsBucket.remove(s3, bucketName);

        // Then
        assertThat(doesBucketExist(s3, bucketName)).isFalse();
    }
}
