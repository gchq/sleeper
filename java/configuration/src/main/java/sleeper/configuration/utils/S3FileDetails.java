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

/**
 * A file found in an S3 bucket.
 *
 * @param bucket        the S3 bucket name
 * @param objectKey     the S3 object key
 * @param fileSizeBytes the size of the file in bytes
 */
public record S3FileDetails(String bucket, String objectKey, long fileSizeBytes) {

    /**
     * Builds a path to this file to be used in a job definition. This includes ingest jobs, bulk import jobs, and
     * submissions to the ingest batcher.
     *
     * @return the path
     */
    public String pathForJob() {
        return bucket() + "/" + objectKey();
    }

    /**
     * Builds a path to this file to be used by Hadoop.
     *
     * @return the path
     */
    public String pathForHadoop() {
        return "s3a://" + bucket() + "/" + objectKey();
    }
}
