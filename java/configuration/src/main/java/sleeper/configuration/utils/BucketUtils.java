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

package sleeper.configuration.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import sleeper.core.util.SplitIntoBatches;

import java.util.List;

/**
 * Util for interacting with S3 buckets.
 */
public class BucketUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BucketUtils.class);

    private BucketUtils() {
    }

    /**
     * Checks if an S3 bucket exists.
     *
     * @param  s3         the client to interact with AWS
     * @param  bucketName the string name of the bucket to check
     * @return            true if bucket exists, false otherwise
     */
    public static boolean doesBucketExist(S3Client s3, String bucketName) {
        try {
            s3.headBucket(builder -> builder
                    .bucket(bucketName)
                    .build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    /**
     * Deletes all objects in an S3 bucket with matching prefix.
     *
     * @param s3Client   the client to interact with AWS
     * @param bucketName the string name of the bucket to check
     * @param prefix     the string prefix of objects to delete
     */
    public static void deleteAllObjectsInBucketWithPrefix(S3Client s3Client, String bucketName, String prefix) {
        LOGGER.info("Deleting all objects in the bucket {} with prefix {}", bucketName, prefix);
        int totalObjectsDeleted = 0;
        for (ListObjectsV2Response response : s3Client.listObjectsV2Paginator(request -> request
                .bucket(bucketName)
                .prefix(prefix + "/")
                //Keys set to 1000 as this is the maximum value permitted by S3 delete action
                .maxKeys(1000))) {

            List<ObjectIdentifier> toDelete = response.contents().stream()
                    .map(software.amazon.awssdk.services.s3.model.S3Object::key)
                    .map(key -> ObjectIdentifier.builder()
                            .key(key)
                            .build())
                    .toList();

            if (!toDelete.isEmpty()) {
                totalObjectsDeleted += deleteObjectsReturnDeletedCount(s3Client, bucketName, toDelete);
            }
        }
        LOGGER.info("A total of {} objects were deleted", totalObjectsDeleted);
    }

    /**
     * Deletes all objects in an S3 bucket with matching prefix and key predicate.
     *
     * @param s3Client           the client to interact with AWS
     * @param bucketName         the string name of the bucket to check
     * @param objectKeysToDelete the list of object keys to delete
     */
    public static void deleteObjectsInBucketFromListOfKeys(S3Client s3Client, String bucketName, List<String> objectKeysToDelete) {
        LOGGER.info("Attempting to delete {} objects in the bucket {}", objectKeysToDelete.size(), bucketName);
        int totalObjectsDeleted = 0;

        //S3 Delete capped at 1000 objects
        for (List<String> batch : SplitIntoBatches.splitListIntoBatchesOf(1000, objectKeysToDelete)) {
            List<ObjectIdentifier> toDelete = batch.stream()
                    .map(key -> ObjectIdentifier.builder()
                            .key(key)
                            .build())
                    .toList();

            if (!toDelete.isEmpty()) {
                totalObjectsDeleted += deleteObjectsReturnDeletedCount(s3Client, bucketName, toDelete);
            }
        }

        LOGGER.info("A total of {} objects were deleted", totalObjectsDeleted);
    }

    private static int deleteObjectsReturnDeletedCount(S3Client s3Client, String bucketName, List<ObjectIdentifier> toDelete) {
        DeleteObjectsResponse deleteResponse = s3Client.deleteObjects(request -> request
                .bucket(bucketName)
                .delete(delete -> delete.objects(toDelete)));
        int successfulDeletes = deleteResponse.deleted().size();
        LOGGER.info("{} objects successfully deleted from S3 bucket: {}", successfulDeletes, bucketName);
        return successfulDeletes;
    }
}
