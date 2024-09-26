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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AutoDeleteS3ObjectsLambda {
    public static final Logger LOGGER = LoggerFactory.getLogger(AutoDeleteS3ObjectsLambda.class);

    private final AmazonS3 s3Client;
    private final int batchSize;

    public AutoDeleteS3ObjectsLambda() {
        this(AmazonS3ClientBuilder.defaultClient(), 100);
    }

    public AutoDeleteS3ObjectsLambda(AmazonS3 s3Client, int batchSize) {
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
                deleteAllObjectsInBucket(bucketName);
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void deleteAllObjectsInBucket(String bucketName) {
        List<String> objectKeysForDeletion = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withMaxKeys(batchSize);
        ListObjectsV2Result result;

        LOGGER.info("Deleting all objects in the bucket {}", bucketName);
        int totalObjectsDeleted = 0;
        do {
            objectKeysForDeletion.clear();
            result = s3Client.listObjectsV2(req);
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                objectKeysForDeletion.add(objectSummary.getKey());
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
