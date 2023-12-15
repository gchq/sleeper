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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class contains a snapshot of files in the state store at a point in time, to be used to build a report.
 */
public class AllFileReferences {
    private final Set<String> filesWithNoReferences;
    private final Set<FileInfo> activeFiles;
    private final boolean moreThanMax;

    public AllFileReferences(Collection<FileInfo> activeFiles, Collection<String> filesWithNoReferences) {
        this(activeFiles, filesWithNoReferences, false);
    }

    public AllFileReferences(Collection<FileInfo> activeFiles, Collection<String> filesWithNoReferences, boolean moreThanMax) {
        this.filesWithNoReferences = new LinkedHashSet<>(filesWithNoReferences);
        this.activeFiles = new LinkedHashSet<>(activeFiles);
        this.moreThanMax = moreThanMax;
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

    public Set<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public Set<FileInfo> getActiveFiles() {
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
