/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.api.resources;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketTaggingRequest;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

@QuarkusTest
class InstancesResourceIT {

    @Inject
    S3Client s3Client;

    @Test
    void shouldReturnBucketWithSleeperComponentConfigBucketTag() {
        String bucketName = "test-config-bucket";
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        s3Client.putBucketTagging(PutBucketTaggingRequest.builder()
                .bucket(bucketName)
                .tagging(Tagging.builder()
                        .tagSet(Tag.builder()
                                .key("SleeperComponent")
                                .value("ConfigBucket")
                                .build())
                        .build())
                .build());

        given()
                .when().get("/api/instances")
                .then()
                .statusCode(200)
                .body("$", hasItem(bucketName));
    }

    @Test
    void shouldNotReturnBucketWithoutSleeperComponentTag() {
        String bucketName = "test-other-bucket";
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

        given()
                .when().get("/api/instances")
                .then()
                .statusCode(200)
                .body("$", not(hasItem(bucketName)));
    }

    @Test
    void shouldNotReturnBucketWithDifferentTagValue() {
        String bucketName = "test-different-tag-bucket";
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        s3Client.putBucketTagging(PutBucketTaggingRequest.builder()
                .bucket(bucketName)
                .tagging(Tagging.builder()
                        .tagSet(Tag.builder()
                                .key("SleeperComponent")
                                .value("OtherComponent")
                                .build())
                        .build())
                .build());

        given()
                .when().get("/api/instances")
                .then()
                .statusCode(200)
                .body("$", not(hasItem(bucketName)));
    }

}