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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.S3Filename;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

/**
 * A batch of files from the state store to be deleted from S3. These are indexed for interaction with the S3 API,
 * to convert to S3 object keys and then back to state store filenames to update the state store.
 */
public class FilesToDelete {
    public static final Logger LOGGER = LoggerFactory.getLogger(FilesToDelete.class);

    private final List<FilesToDeleteInBucket> buckets;

    private FilesToDelete(List<FilesToDeleteInBucket> buckets) {
        this.buckets = buckets;
    }

    /**
     * Indexes files from the state store to prepare for deletion from S3.
     *
     * @param  filenames the filenames from the state store
     * @return           the index
     */
    public static FilesToDelete from(List<String> filenames) {
        Map<String, List<S3Filename>> bucketToFiles = filenames.stream()
                .flatMap(filename -> {
                    try {
                        return Stream.of(S3Filename.parse(filename));
                    } catch (Exception e) {
                        LOGGER.warn("Failed reading filename: {}", filename, e);
                        return Stream.empty();
                    }
                })
                .collect(groupingBy(S3Filename::bucketName));
        return new FilesToDelete(bucketToFiles.entrySet().stream()
                .map(entry -> FilesToDeleteInBucket.from(entry.getKey(), entry.getValue()))
                .toList());
    }

    public List<FilesToDeleteInBucket> getBuckets() {
        return buckets;
    }

}
