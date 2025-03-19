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

import java.util.Set;
import java.util.stream.Stream;

/**
 * Converts a filename from the state store to prepare for deletion from S3.
 *
 * @param filename   the full path of the file as recorded in the state store
 * @param bucketName the bucket name where the file is located
 * @param objectKey  the path within the bucket including the full file name
 */
public record FileToDelete(String filename, String bucketName, String objectKey) {

    private static final Set<String> SCHEMES = Set.of("s3", "s3a");

    /**
     * Reads a filename from the state store.
     *
     * @param  filename path from the state store
     * @return          converted file details
     */
    public static FileToDelete fromFilename(String filename) {
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
        return new FileToDelete(filename,
                filename.substring(bucketNameStart, bucketNameEnd),
                filename.substring(bucketNameEnd + 1));
    }

    /**
     * Returns the location of the associated sketches file within the S3 bucket.
     *
     * @return object key of associated sketches file
     */
    public String sketchesObjectKey() {
        return objectKey.replace(".parquet", ".sketches");
    }

    /**
     * Produces a stream of locations of files stored in S3, for the file held in the state store.
     *
     * @return stream of object keys
     */
    public Stream<String> streamObjectKeys() {
        return Stream.of(objectKey, sketchesObjectKey());
    }

}
