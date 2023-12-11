/*
 * Copyright 2022-2023 Crown Copyright
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class contains a snapshot of files in the state store at a point in time, to be used to build a report.
 */
public class AllFileReferences {
    private final List<String> filesWithNoReferences;
    private final List<FileInfo> activeFiles;
    private final boolean moreThanMax;

    public AllFileReferences(List<FileInfo> activeFiles, List<String> filesWithNoReferences) {
        this(activeFiles, filesWithNoReferences, false);
    }

    public AllFileReferences(List<FileInfo> activeFiles, List<String> filesWithNoReferences, boolean moreThanMax) {
        this.filesWithNoReferences = filesWithNoReferences;
        this.activeFiles = activeFiles;
        this.moreThanMax = moreThanMax;
    }

    public static AllFileReferences fromActiveFilesAndReferenceCounts(
            Stream<FileInfo> activeFiles,
            Stream<FileReferenceCount> fileReferenceCounts) {
        Map<String, List<FileInfo>> referencesByFilename = new LinkedHashMap<>();
        fileReferenceCounts.forEach(counts ->
                referencesByFilename.put(counts.getFilename(), new ArrayList<>()));
        activeFiles.forEach(file -> referencesByFilename
                .computeIfAbsent(file.getFilename(), name -> new ArrayList<>())
                .add(file));
        return new AllFileReferences(
                referencesByFilename.values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList()), referencesByFilename.entrySet().stream()
                .filter(entry -> entry.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList())
        );
    }

    public static AllFileReferences fromActiveFilesAndReadyForGCFiles(
            Stream<FileInfo> activeFiles,
            Stream<String> filesWithNoReferences,
            boolean moreFilesWithNoReferences) {
        return new AllFileReferences(
                activeFiles.collect(Collectors.toList()),
                filesWithNoReferences.collect(Collectors.toList()),
                moreFilesWithNoReferences);
    }

    public static AllFileReferences fromStateStoreWithReadyForGCLimit(StateStore stateStore, int maxFilenamesReadyForGC) throws StateStoreException {
        Iterator<String> filenamesReadyForGC = stateStore.getReadyForGCFilenamesBefore(Instant.ofEpochMilli(Long.MAX_VALUE)).iterator();
        List<String> readyForGC = new ArrayList<>();
        int count = 0;
        while (filenamesReadyForGC.hasNext() && count < maxFilenamesReadyForGC) {
            readyForGC.add(filenamesReadyForGC.next());
            count++;
        }
        boolean moreThanMax = filenamesReadyForGC.hasNext();
        return new AllFileReferences(stateStore.getActiveFiles(), readyForGC, moreThanMax);
    }

    public List<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public List<FileInfo> getActiveFiles() {
        return activeFiles;
    }

    public boolean isMoreThanMax() {
        return moreThanMax;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllFileReferences that = (AllFileReferences) o;
        return moreThanMax == that.moreThanMax && Objects.equals(filesWithNoReferences, that.filesWithNoReferences) && Objects.equals(activeFiles, that.activeFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filesWithNoReferences, activeFiles, moreThanMax);
    }

    @Override
    public String toString() {
        return "AllFileReferences{" +
                "filesWithNoReferences=" + filesWithNoReferences +
                ", activeFiles=" + activeFiles +
                ", moreThanMax=" + moreThanMax +
                '}';
    }
}
