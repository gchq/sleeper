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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.junit.jupiter.api.Test;

import sleeper.localstack.test.LocalStackTestBase;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class AutoDeleteS3ObjectsLambdaIT extends LocalStackTestBase {

    @Test
    void shouldDeleteObjectOnDelete() {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        putObject(bucketName, "test.txt", "some content");

        // When
        lambda().handleEvent(deleteEventForBucket(bucketName), null);

        // Then
        assertThat(listObjectKeys(bucketName)).isEmpty();
    }

    @Test
    void shouldDeleteMoreObjectsThanBatchSizeOnDelete() {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        putObject(bucketName, "test1.txt", "some content");
        putObject(bucketName, "test2.txt", "other content");
        putObject(bucketName, "test3.txt", "more content");
        int batchSize = 2;

        // When
        lambdaWithBatchSize(batchSize).handleEvent(deleteEventForBucket(bucketName), null);

        // Then
        assertThat(listObjectKeys(bucketName)).isEmpty();
    }

    @Test
    void shouldDeleteNoObjectsOnDelete() {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);

        // When
        lambda().handleEvent(deleteEventForBucket(bucketName), null);

        // Then
        assertThat(listObjectKeys(bucketName)).isEmpty();
    }

    @Test
    void shouldDoNothingOnCreate() {
        // Given
        String bucketName = UUID.randomUUID().toString();
        createBucket(bucketName);
        putObject(bucketName, "test.txt", "some content");

        // When
        lambda().handleEvent(createEventForBucket(bucketName), null);

        // Then
        assertThat(listObjectKeys(bucketName)).containsExactly("test.txt");
    }

    private CloudFormationCustomResourceEvent createEventForBucket(String bucketName) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(Map.of("bucket", bucketName))
                .build();
    }

    private CloudFormationCustomResourceEvent deleteEventForBucket(String bucketName) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType("Delete")
                .withResourceProperties(Map.of("bucket", bucketName))
                .build();
    }

    private AutoDeleteS3ObjectsLambda lambda() {
        return lambdaWithBatchSize(10);
    }

    private AutoDeleteS3ObjectsLambda lambdaWithBatchSize(int batchSize) {
        return new AutoDeleteS3ObjectsLambda(s3Client, batchSize);
    }

}
