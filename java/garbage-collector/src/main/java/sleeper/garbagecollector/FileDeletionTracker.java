/*
 * Copyright 2022-2026 Crown Copyright
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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Tracks when all S3 objects for each filename in a bucket have been deleted, or when any of them failed.
 */
class FileDeletionTracker {

    private final FilesToDeleteInBucket files;
    private final Map<String, Set<String>> remainingObjectKeysByFilename = new LinkedHashMap<>();
    private final Set<String> filenamesWithAllObjectsDeleted = new LinkedHashSet<>();
    private final Map<String, Exception> failureByFilename = new LinkedHashMap<>();

    FileDeletionTracker(FilesToDeleteInBucket files) {
        this.files = files;
        files.getObjectKeysByFilename().forEach((filename, objectKeys) -> remainingObjectKeysByFilename.put(filename, new LinkedHashSet<>(objectKeys)));
    }

    /**
     * Records that an S3 object was deleted. If this was the last remaining object for its filename, all objects for
     * that filename have now been deleted.
     *
     * @param objectKey the S3 object key
     */
    void objectDeleted(String objectKey) {
        String filename = files.getFilenameForObjectKey(objectKey);
        Set<String> remainingObjectKeys = remainingObjectKeysByFilename.get(filename);
        remainingObjectKeys.remove(objectKey);
        if (remainingObjectKeys.isEmpty()) {
            filenamesWithAllObjectsDeleted.add(filename);
        }
    }

    /**
     * Records that an S3 object failed to delete. This means its filename will be recorded as failed. Only the first
     * failure is kept for each filename.
     *
     * @param objectKey the S3 object key
     * @param failure   the failure
     */
    void objectFailed(String objectKey, Exception failure) {
        failureByFilename.putIfAbsent(files.getFilenameForObjectKey(objectKey), failure);
    }

    /**
     * Records that a number of S3 objects failed to delete with the same failure, e.g. when a whole request failed.
     *
     * @param objectKeys the S3 object keys
     * @param failure    the failure
     */
    void objectsFailed(List<String> objectKeys, Exception failure) {
        objectKeys.forEach(objectKey -> objectFailed(objectKey, failure));
    }

    /**
     * Records the result for each filename. Filenames are recorded as deleted only when all of their S3 objects were
     * deleted and none failed. Failed filenames are grouped by their failure, so that a failed request is recorded
     * once rather than once per filename.
     *
     * @param deleted the tracker to record results in
     */
    void recordFileResults(TableFilesDeleted deleted) {
        filenamesWithAllObjectsDeleted.stream()
                .filter(filename -> !failureByFilename.containsKey(filename))
                .forEach(deleted::deleted);
        failureByFilename.entrySet().stream()
                .collect(groupingBy(Entry::getValue, LinkedHashMap::new, mapping(Entry::getKey, toList())))
                .forEach((failure, failedFilenames) -> deleted.failed(failedFilenames, failure));
    }
}
