/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.impl.partitionfilewriter;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class TransferManagerUploader implements AsyncS3Uploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransferManagerUploader.class);

    private final AmazonS3 s3;

    public TransferManagerUploader(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public CompletableFuture<?> upload(Path localFile, String s3BucketName, String s3Key) {
        TransferManager transfer = TransferManagerBuilder.standard()
                .withS3Client(s3)
                .build();
        Upload upload = transfer.upload(s3BucketName, s3Key, localFile.toFile());
        CompletableFuture<UploadResult> future = new CompletableFuture<>();
        upload.addProgressListener((ProgressEvent progressEvent) ->
                reportCompletion(progressEvent, localFile, upload, future));
        return future;
    }

    private static void reportCompletion(ProgressEvent progressEvent, Path localFile, Upload upload, CompletableFuture<UploadResult> future) {
        LOGGER.trace("Found progress event for upload of {}: {}", localFile.getFileName(), progressEvent);
        switch (progressEvent.getEventType()) {
            case TRANSFER_COMPLETED_EVENT:
            case TRANSFER_CANCELED_EVENT:
            case TRANSFER_FAILED_EVENT:
                getUploadResultAndComplete(upload, future);
                break;
            default:
        }
    }

    private static void getUploadResultAndComplete(Upload upload, CompletableFuture<UploadResult> future) {
        try {
            UploadResult result = upload.waitForUploadResult();
            future.complete(result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(e);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public void close() {
        s3.shutdown();
    }
}
