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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

public class FilesToDelete {
    public static final Logger LOGGER = LoggerFactory.getLogger(FilesToDelete.class);

    private final List<FilesToDeleteInBucket> buckets;

    private FilesToDelete(List<FilesToDeleteInBucket> buckets) {
        this.buckets = buckets;
    }

    public static FilesToDelete from(List<String> filenames) {
        Map<String, List<FileToDelete>> bucketToFiles = filenames.stream()
                .flatMap(filename -> {
                    try {
                        return Stream.of(FileToDelete.fromFilename(filename));
                    } catch (Exception e) {
                        LOGGER.warn("Failed reading filename: {}", filename, e);
                        return Stream.empty();
                    }
                })
                .collect(groupingBy(FileToDelete::bucketName));
        return new FilesToDelete(bucketToFiles.entrySet().stream()
                .map(entry -> FilesToDeleteInBucket.from(entry.getKey(), entry.getValue()))
                .toList());
    }

    public List<FilesToDeleteInBucket> getBuckets() {
        return buckets;
    }

}
