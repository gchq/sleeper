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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public record FileToDelete(String filename, String bucketName, String objectKey) {

    public static Map<String, List<FileToDelete>> readAndGroupByBucketName(List<String> filenames) {
        return filenames.stream()
                .map(FileToDelete::fromFilename)
                .collect(groupingBy(FileToDelete::bucketName));
    }

    public static FileToDelete fromFilename(String filename) {
        int schemeEnd = filename.indexOf("://") + 3;
        int bucketNameEnd = filename.indexOf("/", schemeEnd);
        return new FileToDelete(filename,
                filename.substring(schemeEnd, bucketNameEnd),
                filename.substring(bucketNameEnd + 1));
    }

    public String sketchesObjectKey() {
        return objectKey.replace(".parquet", ".sketches");
    }

    public Stream<String> streamObjectKeys() {
        return Stream.of(objectKey, sketchesObjectKey());
    }

}
