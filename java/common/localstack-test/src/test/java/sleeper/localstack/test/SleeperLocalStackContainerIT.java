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
package sleeper.localstack.test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.localstack.test.LocalStackAwsV1ClientHelper.buildAwsV1Client;

@Testcontainers
public class SleeperLocalStackContainerIT {

    @Container
    public static LocalStackContainer localStackContainer = SleeperLocalStackContainer.create(LocalStackContainer.Service.S3);

    @Test
    void shouldCreateS3Bucket() {
        AmazonS3 s3Client = buildAwsV1Client(localStackContainer, LocalStackContainer.Service.S3, AmazonS3ClientBuilder.standard());

        String bucketName = UUID.randomUUID().toString();
        s3Client.createBucket(bucketName);
        assertThat(s3Client.listObjects(bucketName).getObjectSummaries()).isEmpty();
    }

}
