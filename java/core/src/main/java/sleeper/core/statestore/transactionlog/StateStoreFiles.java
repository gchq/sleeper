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
import sleeper.core.statestore.AssignJobIdRequest;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.exception.FileAlreadyExistsException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class StateStoreFiles {
    private final Map<String, AllReferencesToAFile> filesByFilename = new TreeMap<>();

    public Stream<FileReference> references() {
        return filesByFilename.values().stream()
                .flatMap(file -> file.getInternalReferences().stream());
    }

    public Stream<AllReferencesToAFile> referencedAndUnreferenced() {
        return filesByFilename.values().stream();
    }

    public Stream<String> unreferencedBefore(Instant maxUpdateTime) {
        return filesByFilename.values().stream()
                .filter(file -> file.getTotalReferenceCount() == 0)
                .filter(file -> file.getLastStateStoreUpdateTime().isBefore(maxUpdateTime))
                .map(AllReferencesToAFile::getFilename);
    }

    public boolean isEmpty() {
        return filesByFilename.isEmpty();
    }

    void add(Iterable<AllReferencesToAFile> files) {
        for (AllReferencesToAFile file : files) {
            filesByFilename.put(file.getFilename(), file);
        }
    }

    public void validateNewFiles(List<AllReferencesToAFile> files) throws FileAlreadyExistsException {
        for (AllReferencesToAFile file : files) {
            if (filesByFilename.containsKey(file.getFilename())) {
                throw new FileAlreadyExistsException(file.getFilename());
            }
        }
    }

    void assignJobIds(List<AssignJobIdRequest> requests, Instant updateTime) {
        for (AssignJobIdRequest request : requests) {
            for (String filename : request.getFilenames()) {
                AllReferencesToAFile file = filesByFilename.get(filename);
                AllReferencesToAFile updated = file.withJobIdForPartition(
                        request.getJobId(), request.getPartitionId(), updateTime);
                filesByFilename.put(filename, updated);
            }
        }
    }

    void replaceFiles(String partitionId, List<String> removeFiles, FileReference newReference, Instant updateTime) {
        for (String filename : removeFiles) {
            AllReferencesToAFile file = filesByFilename.get(filename);
            AllReferencesToAFile updated = file.removeReferenceForPartition(partitionId, updateTime);
            filesByFilename.put(filename, updated);
        }
        add(() -> AllReferencesToAFile.newFilesWithReferences(Stream.of(newReference), updateTime).iterator());
    }

}
