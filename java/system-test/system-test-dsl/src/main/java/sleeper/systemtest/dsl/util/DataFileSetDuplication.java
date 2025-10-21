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
package sleeper.systemtest.dsl.util;

import sleeper.core.statestore.FileReference;
import sleeper.systemtest.dsl.instance.DataFilesDriver;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public record DataFileSetDuplication(List<FileReference> originalReferences, List<FileReference> newReferences) {

    public static List<DataFileSetDuplication> duplicateByReferences(DataFilesDriver driver, int duplicates, List<FileReference> fileReferences) {
        Map<String, List<FileReference>> referencesByFilename = fileReferences.stream()
                .collect(Collectors.groupingBy(FileReference::getFilename, TreeMap::new, Collectors.toUnmodifiableList()));
        List<DataFileDuplication> results = driver.duplicateFiles(duplicates, referencesByFilename.keySet());
        return IntStream.range(0, duplicates)
                .mapToObj(i -> new DataFileSetDuplication(fileReferences, results.stream()
                        .flatMap(result -> result.duplicateFileReferences(referencesByFilename.get(result.originalFilename())))
                        .toList()))
                .toList();
    }

}
