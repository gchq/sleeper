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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.ThreadSleep;
import sleeper.garbagecollector.GarbageCollector.DeleteFiles;

import java.util.List;

/**
 * Deletes files from S3 by their filename in the state store. Used during garbage collection.
 */
public class S3DeleteFiles implements DeleteFiles {
    public static final Logger LOGGER = LoggerFactory.getLogger(S3DeleteFiles.class);

    private final AmazonS3 s3Client;
    private final int s3BatchSize;
    private final ThreadSleep sleep;

    public S3DeleteFiles(AmazonS3 s3Client) {
        this(s3Client, 1000, Thread::sleep);
    }

    public S3DeleteFiles(AmazonS3 s3Client, int s3BatchSize, ThreadSleep sleep) {
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
                DeleteObjectsResult result = s3Client.deleteObjects(deleteRequest);
                for (DeletedObject object : result.getDeletedObjects()) {
                    deleted.deleted(filesInBucket.getFilenameForObjectKey(object.getKey()));
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
        return e instanceof AmazonS3Exception s3e
                && "SlowDown".equals(s3e.getErrorCode());
    }

    private static DeleteObjectsRequest createDeleteObjectsRequest(FilesToDeleteInBucket filesInBucket, List<String> objectKeys) {
        return new DeleteObjectsRequest(filesInBucket.bucketName())
                .withKeys(objectKeys.stream().map(objectKey -> new KeyVersion(objectKey)).toList());
    }

}
