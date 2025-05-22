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

package sleeper.clients.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class BucketUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(BucketUtils.class);

    private BucketUtils() {
    }

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

    public static void deleteObjectsInBucketWithPrefix(S3Client s3Client, String bucketName, String prefix, Predicate<String> keyPredicate) {
        LOGGER.info("Deleting all objects in the bucket {} with prefix {}", bucketName, prefix);
        int totalObjectsDeleted = 0;
        for (ListObjectsV2Response response : s3Client.listObjectsV2Paginator(request -> request
                .bucket(bucketName)
                .prefix(prefix + "/")
                //Keys set to 1000 as this is the maximum value permitted by S3 delete action
                .maxKeys(1000))) {

            List<ObjectIdentifier> toDelete = response.contents().stream()
                    .map(software.amazon.awssdk.services.s3.model.S3Object::key)
                    .filter(keyPredicate)
                    .map(key -> ObjectIdentifier.builder()
                            .key(key)
                            .build())
                    .toList();

            if (!toDelete.isEmpty()) {
                DeleteObjectsResponse deleteResponse = s3Client.deleteObjects(request -> request
                        .bucket(bucketName)
                        .delete(delete -> delete.objects(toDelete)));
                int successfulDeletes = deleteResponse.deleted().size();
                LOGGER.info("{} objects successfully deleted from S3 bucket: {}", successfulDeletes, bucketName);
                totalObjectsDeleted += successfulDeletes;
            }
        }
        LOGGER.info("A total of {} objects were deleted", totalObjectsDeleted);
    }

    public static void deleteAllObjectsInBucketWithPrefix(AmazonS3 s3Client, String bucketName, String prefix) {
        deleteObjectsInBucketWithPrefix(s3Client, bucketName, prefix, key -> true);
    }

    public static void deleteObjectsInBucketWithPrefix(AmazonS3 s3Client, String bucketName, String prefix, Predicate<String> keyPredicate) {
        List<String> objectKeysForDeletion = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix + "/")
                //Keys set to 1000 as this is the maximum value permitted by S3 delete action
                .withMaxKeys(1000);
        ListObjectsV2Result result;

        LOGGER.info("Deleting all objects in the bucket {} with prefix {}", bucketName, prefix);
        int totalObjectsDeleted = 0;
        do {
            objectKeysForDeletion.clear();
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                if (keyPredicate.test(objectSummary.getKey())) {
                    objectKeysForDeletion.add(objectSummary.getKey());
                }
            }
            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
            totalObjectsDeleted += deleteObjects(s3Client, bucketName, objectKeysForDeletion);
        } while (result.isTruncated());
        LOGGER.info("A total of {} objects were deleted", totalObjectsDeleted);
    }

    private static int deleteObjects(AmazonS3 s3Client, String bucketName, List<String> keys) {
        int successfulDeletes = 0;
        if (!keys.isEmpty()) {
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                    .withKeys(keys.toArray(new String[0]))
                    .withQuiet(false);
            DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
            successfulDeletes = delObjRes.getDeletedObjects().size();
            LOGGER.info("{} objects successfully deleted from S3 bucket: {}", successfulDeletes, bucketName);
        }
        return successfulDeletes;
    }
}
