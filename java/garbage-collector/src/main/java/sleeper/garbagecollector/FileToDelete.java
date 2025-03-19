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

public record FileToDelete(String filename, String bucketName, String objectKey) {

    private static final Set<String> SCHEMES = Set.of("s3", "s3a");

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

    public String sketchesObjectKey() {
        return objectKey.replace(".parquet", ".sketches");
    }

    public Stream<String> streamObjectKeys() {
        return Stream.of(objectKey, sketchesObjectKey());
    }

}
