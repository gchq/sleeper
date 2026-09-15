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

package sleeper.systemtest.drivers.nightly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Objects;

public class NightlyTestUploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(NightlyTestUploader.class);

    private final S3Client s3Client;
    private final String bucketName;
    private final NightlyTestTimestamp timestamp;
    private final String prefix;

    private NightlyTestUploader(Builder builder) {
        s3Client = Objects.requireNonNull(builder.s3Client, "s3Client must not be null");
        bucketName = Objects.requireNonNull(builder.bucketName, "bucketName must not be null");
        timestamp = Objects.requireNonNull(builder.timestamp, "timestamp must not be null");
        prefix = timestamp.getS3FolderName();
    }

    public static Builder builder() {
        return new Builder();
    }

    public void upload(NightlyTestOutput output) {
        LOGGER.info("Uploading to S3 bucket and folder: {}/{}", bucketName, prefix);
        // Attempt every file even if some fail, so that one failure doesn't lose the remaining output
        // or stop the summary table below from being updated. Failures are reported afterwards.
        List<NightlyTestUploadFile> failedUploads = output.uploads().parallel()
                .filter(file -> !upload(file))
                .toList();
        NightlyTestSummaryTable.fromS3(s3Client, bucketName)
                .add(timestamp, output)
                .saveToS3(s3Client, bucketName);
        if (!failedUploads.isEmpty()) {
            throw new UploadFailedException(failedUploads);
        }
    }

    /**
     * Uploads a single file to S3.
     *
     * @param  file the file to upload
     * @return      true if the file was uploaded, false if it failed
     */
    public boolean upload(NightlyTestUploadFile file) {
        LOGGER.info("Uploading {}", file);
        try {
            s3Client.putObject(
                    request -> request.bucket(bucketName).key(prefix + "/" + file.getRelativeS3Key()),
                    file.getFile());
            LOGGER.info("Uploaded {}", file);
            return true;
        } catch (SdkException e) {
            LOGGER.error("Failed to upload {}", file.getRelativeS3Key(), e);
            return false;
        }
    }

    /**
     * Thrown when some files could not be uploaded. This is reported after the summary table has been
     * updated, so that the failure is still visible rather than silently losing test output.
     */
    public static class UploadFailedException extends RuntimeException {

        UploadFailedException(List<NightlyTestUploadFile> failedUploads) {
            super("Failed to upload " + failedUploads.size() + " file(s): " + failedUploads);
        }
    }

    public static final class Builder {
        private S3Client s3Client;
        private String bucketName;
        private NightlyTestTimestamp timestamp;

        private Builder() {
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder timestamp(NightlyTestTimestamp timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public NightlyTestUploader build() {
            return new NightlyTestUploader(this);
        }
    }
}
