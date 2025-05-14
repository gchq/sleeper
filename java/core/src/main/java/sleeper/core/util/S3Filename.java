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
package sleeper.core.util;

import java.util.Set;

/**
 * Parses a filename as held in the state store to interact with it in S3.
 *
 * @param filename   the full path of the file as recorded in the state store
 * @param bucketName the bucket name where the file is located
 * @param objectKey  the path within the bucket including the file name and extension
 */
public record S3Filename(String filename, String bucketName, String objectKey) {

    private static final Set<String> SCHEMES = Set.of("s3", "s3a");

    /**
     * Parses a filename from the state store.
     *
     * @param  filename the filename from the state store
     * @return          the parsed details
     */
    public static S3Filename parse(String filename) {
        int schemeEnd = filename.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("Filename is missing scheme");
        }
        String scheme = filename.substring(0, schemeEnd);
        if (!SCHEMES.contains(scheme)) {
            throw new IllegalArgumentException("Unexpected scheme: " + scheme);
        }
        int bucketNameStart = schemeEnd + 3;
        int bucketNameEnd = filename.indexOf("/", bucketNameStart);
        if (bucketNameEnd < 0) {
            throw new IllegalArgumentException("Filename is missing object key");
        }
        return new S3Filename(filename,
                filename.substring(bucketNameStart, bucketNameEnd),
                filename.substring(bucketNameEnd + 1));
    }

}
