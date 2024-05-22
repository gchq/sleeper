/*
 * Copyright 2022-2024 Crown Copyright
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

package sleeper.core.statestore;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AllReferencesToAFileTestHelper {

    private AllReferencesToAFileTestHelper() {
    }

    public static AllReferencesToAFile fileWithNoReferences(String filename) {
        return fileWithNoReferences(filename, null);
    }

    public static AllReferencesToAFile fileWithNoReferences(String filename, Instant updateTime) {
        return AllReferencesToAFile.builder()
                .filename(filename)
                .internalReferences(List.of())
                .totalReferenceCount(0)
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    public static AllReferencesToAFile fileWithReferences(FileReference... references) {
        return fileWithReferences(List.of(references));
    }

    public static AllReferencesToAFile fileWithReferences(Collection<FileReference> references) {
        List<AllReferencesToAFile> files = AllReferencesToAFile
                .newFilesWithReferences(references.stream())
                .collect(Collectors.toUnmodifiableList());
        if (files.size() != 1) {
            throw new IllegalArgumentException("Expected one file, found: " + files);
        }
        return files.get(0);
    }

}
