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
 * A location in S3 to look for files.
 *
 * @param requestedPath the path requested before parsing
 * @param bucket        the S3 bucket name
 * @param prefix        prefix for object keys
 */
public record S3Path(String requestedPath, String bucket, String prefix) {

    /**
     * Parses a path from a request in an ingest job, bulk import job or ingest batcher submission.
     *
     * @param  path the path
     * @return      the parsed location in S3
     */
    public static S3Path parse(String path) {
        if (!path.contains("/")) {
            return new S3Path(path, path, "");
        } else {
            String bucketPath = path.contains("//") ? path.substring(path.indexOf("//") + 2) : path;

            return new S3Path(path,
                    bucketPath.substring(0, bucketPath.indexOf("/")),
                    bucketPath.substring(bucketPath.indexOf("/") + 1));
        }
    }
}
