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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AutoDeleteS3ObjectsLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoDeleteS3ObjectsLambda.class);

    private final S3Client s3Client;
    private final int batchSize;

    public AutoDeleteS3ObjectsLambda() {
        this(S3Client.create(), 1000);
    }

    public AutoDeleteS3ObjectsLambda(S3Client s3Client, int batchSize) {
        this.s3Client = s3Client;
        this.batchSize = batchSize;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String bucketName = (String) resourceProperties.get("bucket");

        switch (event.getRequestType()) {
            case "Create":
            case "Update":
                break;
            case "Delete":
                emptyBucket(bucketName);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void emptyBucket(String bucketName) {
        try {
            LOGGER.info("Emptying bucket {}", bucketName);
            s3Client.listObjectVersionsPaginator(builder -> builder.bucket(bucketName).maxKeys(batchSize))
                    .stream().parallel()
                    .forEach(response -> {
                        deleteVersions(bucketName, response);
                        deleteMarkers(bucketName, response);
                    });
        } catch (NoSuchBucketException e) {
            LOGGER.info("Bucket not found: {}", bucketName);
        }
    }

    private void deleteVersions(String bucketName, ListObjectVersionsResponse response) {
        if (!response.versions().isEmpty()) {
            LOGGER.info("Deleting {} versions", response.versions().size());
            s3Client.deleteObjects(builder -> builder.bucket(bucketName)
                    .delete(deleteBuilder -> deleteBuilder
                            .objects(objectIdentifiers(response.versions(), ObjectVersion::key, ObjectVersion::versionId))));
        }

    }

    private void deleteMarkers(String bucketName, ListObjectVersionsResponse response) {
        if (!response.deleteMarkers().isEmpty()) {
            LOGGER.info("Deleting {} delete markers", response.deleteMarkers().size());
            s3Client.deleteObjects(builder -> builder.bucket(bucketName)
                    .delete(deleteBuilder -> deleteBuilder
                            .objects(objectIdentifiers(response.deleteMarkers(), DeleteMarkerEntry::key, DeleteMarkerEntry::versionId))));
        }
    }

    private static <T> Collection<ObjectIdentifier> objectIdentifiers(
            List<T> versions, Function<T, String> getKey, Function<T, String> getVersionId) {
        return versions.stream()
                .map(version -> ObjectIdentifier.builder()
                        .key(getKey.apply(version))
                        .versionId(getVersionId.apply(version)).build())
                .collect(Collectors.toList());
    }
}
