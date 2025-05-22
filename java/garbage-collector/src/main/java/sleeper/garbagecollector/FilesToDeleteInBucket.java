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

import sleeper.core.util.S3Filename;
import sleeper.core.util.SplitIntoBatches;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Files from the state store to be deleted from a single S3 bucket. These are indexed for interaction with the S3 API,
 * to convert to S3 object keys and then back to state store filenames to update the state store.
 *
 * @param bucketName          the name of the S3 bucket
 * @param objectKeyToFilename a map from S3 object key to the filename in the state store
 */
public record FilesToDeleteInBucket(String bucketName, Map<String, String> objectKeyToFilename) {

    /**
     * Indexes files from the state store to prepare for deletion from S3.
     *
     * @param  bucketName the name of the S3 bucket
     * @param  files      the parsed state store filenames
     * @return            the index
     */
    public static FilesToDeleteInBucket from(String bucketName, List<S3Filename> files) {
        Map<String, String> objectKeyToFilename = files.stream()
                .flatMap(file -> streamObjectKeys(file).map(key -> Map.entry(key, file.filename())))
                .collect(toMap(Entry::getKey, Entry::getValue));
        return new FilesToDeleteInBucket(bucketName, objectKeyToFilename);
    }

    private static Stream<String> streamObjectKeys(S3Filename filename) {
        return Stream.of(filename.objectKey(), filename.sketchesObjectKey());
    }

    public Collection<String> getObjectKeys() {
        return objectKeyToFilename.keySet();
    }

    public Stream<List<String>> objectKeysInBatchesOf(int batchSize) {
        return SplitIntoBatches.streamBatchesOf(batchSize, objectKeyToFilename.keySet().stream());
    }

    public String getFilenameForObjectKey(String objectKey) {
        return objectKeyToFilename.get(objectKey);
    }

    public List<String> getAllFilenamesInBatch(List<String> objectKeysForBatch) {
        return objectKeysForBatch.stream().map(this::getFilenameForObjectKey).distinct().toList();
    }

}
