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

/**
 * A test helper to create file records for a state store.
 */
public class AllReferencesToAFileTestHelper {

    private AllReferencesToAFileTestHelper() {
    }

    /**
     * Creates a file record with no references. This will be as it is before being added to the state store, with no
     * update time.
     *
     * @param  filename the filename
     * @return          the file
     */
    public static AllReferencesToAFile fileWithNoReferences(String filename) {
        return fileWithNoReferences(filename, null);
    }

    /**
     * Creates a file record with no references, last updated at a certain time.
     *
     * @param  filename   the filename
     * @param  updateTime the last time the file was updated in the state store
     * @return            the file
     */
    public static AllReferencesToAFile fileWithNoReferences(String filename, Instant updateTime) {
        return AllReferencesToAFile.builder()
                .filename(filename)
                .references(List.of())
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    /**
     * Creates a record for a file referenced in a single partition.
     *
     * @param  reference  the reference to the file
     * @param  updateTime the last time the file was updated in the state store
     * @return            the file
     */
    public static AllReferencesToAFile fileWithOneReference(FileReference reference, Instant updateTime) {
        return AllReferencesToAFile.builder()
                .filename(reference.getFilename())
                .references(List.of(reference.toBuilder().lastStateStoreUpdateTime(updateTime).build()))
                .lastStateStoreUpdateTime(updateTime)
                .build();
    }

    /**
     * Creates a file record with given references. This will be as it is before being added to the state store, with no
     * update time.
     *
     * @param  references the references
     * @return            the file
     */
    public static AllReferencesToAFile fileWithReferences(FileReference... references) {
        return fileWithReferences(List.of(references));
    }

    /**
     * Creates a file record with given references. This will be as it is before being added to the state store, with no
     * update time.
     *
     * @param  references the references
     * @return            the file
     */
    public static AllReferencesToAFile fileWithReferences(Collection<FileReference> references) {
        List<AllReferencesToAFile> files = filesWithReferences(references);
        if (files.size() != 1) {
            throw new IllegalArgumentException("Expected one file, found: " + files);
        }
        return files.get(0);
    }

    /**
     * Creates a list of files with given references. This will be as it is before being added to the state store, with
     * no update time.
     *
     * @param  references the references
     * @return            the file
     */
    public static List<AllReferencesToAFile> filesWithReferences(Collection<FileReference> references) {
        return AllReferencesToAFile.newFilesWithReferences(references);
    }

}
