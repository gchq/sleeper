/*
 * Copyright 2022-2023 Crown Copyright
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

import com.amazonaws.services.s3.AmazonS3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Objects;

public class NightlyTestUploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(NightlyTestUploader.class);

    private final AmazonS3 s3Client;
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
        output.filesToUpload().parallel().forEach(this::upload);
        NightlyTestSummaryTable.fromS3(s3Client, bucketName)
                .add(timestamp, output)
                .saveToS3(s3Client, bucketName);
    }

    public void upload(Path file) {
        String filename = Objects.toString(file.getFileName());
        LOGGER.info("Uploading {}", filename);
        s3Client.putObject(bucketName, prefix + "/" + filename, file.toFile());
        LOGGER.info("Uploaded {}", filename);
    }

    public static final class Builder {
        private AmazonS3 s3Client;
        private String bucketName;
        private NightlyTestTimestamp timestamp;

        private Builder() {
        }

        public Builder s3Client(AmazonS3 s3Client) {
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
