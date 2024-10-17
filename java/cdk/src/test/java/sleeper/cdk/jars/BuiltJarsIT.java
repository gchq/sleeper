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
package sleeper.cdk.jars;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

import sleeper.cdk.testutils.LocalStackTestBase;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class BuiltJarsIT extends LocalStackTestBase {

    private final String bucketName = UUID.randomUUID().toString();
    private final BuiltJars builtJars = new BuiltJars(s3Client, bucketName);

    @Test
    void shouldGetLatestVersionOfAJar() {
        createBucket(bucketName);
        s3Client.putBucketVersioning(put -> put.bucket(bucketName)
                .versioningConfiguration(config -> config.status(BucketVersioningStatus.ENABLED)));
        String versionId = putObject(bucketName, "test.jar", "data").versionId();

        assertThat(builtJars.getLatestVersionId(LambdaJar.fromFormat("test.jar")))
                .isEqualTo(versionId);
        assertThat(versionId).isNotNull();
    }
}
