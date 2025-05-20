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
package sleeper.garbagecollector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import sleeper.core.util.ThreadSleep;
import sleeper.garbagecollector.GarbageCollector.DeleteFiles;

import java.util.List;

/**
 * Deletes files from S3 by their filename in the state store. Used during garbage collection.
 */
public class S3DeleteFiles implements DeleteFiles {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3DeleteFiles.class);

    private final S3Client s3Client;
    private final int s3BatchSize;
    private final ThreadSleep sleep;

    public S3DeleteFiles(S3Client s3Client) {
        this(s3Client, 1000, Thread::sleep);
    }

    public S3DeleteFiles(S3Client s3Client, int s3BatchSize, ThreadSleep sleep) {
        this.s3Client = s3Client;
        this.s3BatchSize = s3BatchSize;
        this.sleep = sleep;
    }

    @Override
    public void deleteFiles(List<String> filenames, TableFilesDeleted deleted) {
        FilesToDelete files = FilesToDelete.from(filenames);
        for (FilesToDeleteInBucket filesInBucket : files.getBuckets()) {
            filesInBucket.objectKeysInBatchesOf(s3BatchSize)
                    .forEach(batch -> deleteS3Batch(filesInBucket, batch, deleted));
        }
    }

    private void deleteS3Batch(FilesToDeleteInBucket filesInBucket, List<String> objectKeys, TableFilesDeleted deleted) {
        DeleteObjectsRequest deleteRequest = createDeleteObjectsRequest(filesInBucket, objectKeys);
        boolean retry;
        do {
            retry = false;
            try {
                LOGGER.debug("Sending request to delete {} objects", objectKeys.size());
                DeleteObjectsResponse response = s3Client.deleteObjects(deleteRequest);
                for (DeletedObject object : response.deleted()) {
                    deleted.deleted(filesInBucket.getFilenameForObjectKey(object.key()));
                }
            } catch (Exception e) {
                if (isSlowDownException(e)) {
                    retry = true;
                    LOGGER.warn("Found slow down response, retrying after 1s");
                    sleep.waitForMillisWrappingInterrupt(1000);
                } else {
                    LOGGER.error("Failed to delete batch: {}", objectKeys, e);
                    deleted.failed(filesInBucket.getAllFilenamesInBatch(objectKeys), e);
                }
            }
        } while (retry);
    }

    private static boolean isSlowDownException(Exception e) {
        return e instanceof AwsServiceException s3e
                && "SlowDown".equals(s3e.awsErrorDetails().errorCode());
    }

    private static DeleteObjectsRequest createDeleteObjectsRequest(FilesToDeleteInBucket filesInBucket, List<String> objectKeys) {
        return DeleteObjectsRequest.builder()
                .bucket(filesInBucket.bucketName())
                .delete(Delete.builder()
                        .objects(objectKeys.stream()
                                .map(objectKey -> ObjectIdentifier.builder()
                                        .key(objectKey)
                                        .build())
                                .toList())
                        .build())
                .build();
    }

}
