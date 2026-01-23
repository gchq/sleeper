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
 * @param pathInBucket  the path in the bucket, either a prefix or a full object key
 */
public record S3Path(String requestedPath, String bucket, String pathInBucket) {

    /**
     * Parses a path from a request in an ingest job, bulk import job or ingest batcher submission.
     *
     * @param  path the path
     * @return      the parsed location in S3
     */
    public static S3Path parse(String path) {
        String bucketPath = pathStartsWithScheme(path) ? path.substring(path.indexOf("//") + 2) : path;

        if (!bucketPath.contains("/")) {
            return new S3Path(path, bucketPath, "");
        } else {
            int firstSlashLocation = bucketPath.indexOf("/");
            return new S3Path(path,
                    bucketPath.substring(0, firstSlashLocation),
                    getPathInBucketAfterSlashes(bucketPath.substring(firstSlashLocation + 1)));
        }
    }

    private static boolean pathStartsWithScheme(String path) {
        return path.startsWith("s3://") || path.startsWith("s3a://");
    }

    private static String getPathInBucketAfterSlashes(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }
}
