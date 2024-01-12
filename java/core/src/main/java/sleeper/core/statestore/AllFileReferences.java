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

import java.util.Objects;
import java.util.Set;

/**
 * This class contains a snapshot of files in the state store at a point in time, to be used to build a report.
 */
public class AllFileReferences {
    private final Set<String> filesWithNoReferences;
    private final Set<FileReference> activeFiles;
    private final boolean moreThanMax;

    public AllFileReferences(Set<FileReference> activeFiles, Set<String> filesWithNoReferences, boolean moreThanMax) {
        this.filesWithNoReferences = filesWithNoReferences;
        this.activeFiles = activeFiles;
        this.moreThanMax = moreThanMax;
    }

    public Set<String> getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public Set<FileReference> getActiveFiles() {
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
