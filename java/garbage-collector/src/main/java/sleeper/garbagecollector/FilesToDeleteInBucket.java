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
import java.util.Map.Entry;

import static java.util.stream.Collectors.toMap;

public record FilesToDeleteInBucket(String bucketName, List<String> objectKeys, Map<String, String> objectKeyToFilename) {

    public static FilesToDeleteInBucket from(String bucketName, List<FileToDelete> files) {
        List<String> objectKeys = files.stream().flatMap(FileToDelete::streamObjectKeys).toList();
        Map<String, String> objectKeyToFilename = files.stream()
                .flatMap(file -> file.streamObjectKeys().map(key -> Map.entry(key, file.filename())))
                .collect(toMap(Entry::getKey, Entry::getValue));
        return new FilesToDeleteInBucket(bucketName, objectKeys, objectKeyToFilename);
    }

    public String getBucketName() {
        return bucketName;
    }

    public List<String> getObjectKeys() {
        return objectKeys;
    }

    public Map<String, String> getObjectKeyToFilename() {
        return objectKeyToFilename;
    }

    public String getFilenameForObjectKey(String objectKey) {
        return objectKeyToFilename.get(objectKey);
    }

    public List<String> getAllFilenames() {
        return objectKeys.stream().map(this::getFilenameForObjectKey).distinct().toList();
    }

}
