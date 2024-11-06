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
import sleeper.core.statestore.FileReference;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toUnmodifiableList;

/**
 * Holds the state of file references, for a state store backed by a transaction log. This object is mutable, is cached
 * in memory in the state store, and is updated by applying each transaction in the log in sequence. This is not thread
 * safe.
 * <p>
 * The methods to update this object should only ever be called by the transactions.
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
     * Streams through filenames of all files that have had no references for a certain period of time. This is done by
     * comparing the last update time of each unreferenced file.
     *
     * @param  maxUpdateTime the latest update time to include
     * @return               filenames of unreferenced files updated before the specified time
     */
    public Stream<String> unreferencedBefore(Instant maxUpdateTime) {
        return filesByFilename.values().stream()
                .filter(file -> file.getReferences().size() == 0)
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
    public void add(AllReferencesToAFile file) {
        filesByFilename.put(file.getFilename(), StateStoreFile.from(file));
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
