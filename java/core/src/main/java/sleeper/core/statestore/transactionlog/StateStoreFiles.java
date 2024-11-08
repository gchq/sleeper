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
package sleeper.core.statestore.transactionlog;

import sleeper.core.statestore.AllReferencesToAFile;
import sleeper.core.statestore.AllReferencesToAllFiles;
import sleeper.core.statestore.FileReference;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Holds the state of file references, for a state store implementation that involves mutating the state in-memory. This
 * object is mutable, and may be cached in memory by the state store. This is not thread safe.
 * <p>
 * The transaction log state store caches this in memory and updates the state by applying each transaction from the log
 * in sequence. This class should allow these transactions to be applied efficiently in-place with few copy operations.
 * <p>
 * The methods to update this object should only ever be called by a state store implementation. As such, this object
 * should not be exposed outside of the state store. The full state of files in the state store may be exposed by
 * creating an instance of {@link AllReferencesToAllFiles}.
 */
public class StateStoreFiles {
    private final Map<String, StateStoreFile> filesByFilename = new TreeMap<>();

    /**
     * Streams through all references to all files in the state store.
     *
     * @return all file references
     */
    public Stream<FileReference> references() {
        return filesByFilename.values().stream()
                .flatMap(file -> file.getReferences().stream());
    }

    /**
     * Streams through all files in the state store.
     *
     * @return all files
     */
    public Collection<StateStoreFile> referencedAndUnreferenced() {
        return filesByFilename.values();
    }

    /**
     * Retrieves all references to all files, with a limit on the number of unreferenced files.
     *
     * @param  maxUnreferenced the number of unreferenced files to include
     * @return                 the report
     */
    public AllReferencesToAllFiles allReferencesToAllFiles(int maxUnreferenced) {
        List<AllReferencesToAFile> files = new ArrayList<>();
        int foundUnreferenced = 0;
        boolean moreThanMax = false;
        for (StateStoreFile file : referencedAndUnreferenced()) {
            if (file.getReferences().isEmpty()) {
                if (foundUnreferenced >= maxUnreferenced) {
                    moreThanMax = true;
                    continue;
                } else {
                    foundUnreferenced++;
                }
            }
            files.add(file.toModel());
        }
        return new AllReferencesToAllFiles(files, moreThanMax);
    }

    /**
     * Retrieves all references to all files.
     *
     * @return the report
     */
    public AllReferencesToAllFiles allReferencesToAllFiles() {
        return new AllReferencesToAllFiles(referencedAndUnreferenced().stream().map(StateStoreFile::toModel).toList(), false);
    }

    /**
     * Streams through filenames of all files that have had no references for a certain period of time. This is done by
     * comparing the last update time of each unreferenced file.
     *
     * @param  maxUpdateTime the latest update time to include
     * @return               filenames of unreferenced files updated before the specified time
     */
    public Stream<String> unreferencedBefore(Instant maxUpdateTime) {
        return filesByFilename.values().stream()
                .filter(file -> file.getReferences().isEmpty())
                .filter(file -> file.getLastStateStoreUpdateTime().isBefore(maxUpdateTime))
                .map(StateStoreFile::getFilename)
                .collect(toUnmodifiableList()).stream(); // Avoid concurrent modification during GC
    }

    /**
     * Retreives all information held about a specific file.
     *
     * @param  filename the filename
     * @return          the file if it exists in the state store
     */
    public Optional<StateStoreFile> file(String filename) {
        return Optional.ofNullable(filesByFilename.get(filename));
    }

    public boolean isEmpty() {
        return filesByFilename.isEmpty();
    }

    /**
     * Adds a file to the state. Should only be called by a transaction.
     *
     * @param file the file
     */
    public void add(StateStoreFile file) {
        filesByFilename.put(file.getFilename(), file);
    }

    /**
     * Removes a file from the state. Should only be called by a transaction.
     *
     * @param filename the filename
     */
    public void remove(String filename) {
        filesByFilename.remove(filename);
    }

    /**
     * Deletes all files and empties the state. Should only be called by a transaction.
     */
    public void clear() {
        filesByFilename.clear();
    }

    /**
     * Performs some update to the state of a file. Should only be called by a transaction.
     *
     * @param filename the filename
     * @param update   the update
     */
    public void updateFile(String filename, Consumer<StateStoreFile> update) {
        update.accept(filesByFilename.get(filename));
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesByFilename);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StateStoreFiles)) {
            return false;
        }
        StateStoreFiles other = (StateStoreFiles) obj;
        return Objects.equals(filesByFilename, other.filesByFilename);
    }

    @Override
    public String toString() {
        return "StateStoreFiles{filesByFilename=" + filesByFilename + "}";
    }

}
