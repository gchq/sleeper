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
import java.util.List;
import java.util.Objects;

/**
 * Reports on all the references for an individual physical file. A file may be referenced in a number of different
 * partitions, and may also have other external references which contribute to a combined reference count.
 */
public class FileReferences {

    private final String filename;
    private final Instant lastUpdateTime;
    private final int totalReferenceCount;
    private final List<FileReference> references;

    private FileReferences(Builder builder) {
        filename = builder.filename;
        lastUpdateTime = builder.lastUpdateTime;
        totalReferenceCount = builder.totalReferenceCount;
        references = builder.references;
    }

    public String getFilename() {
        return filename;
    }

    public Instant getLastUpdateTime() {
        return lastUpdateTime;
    }

    public int getTotalReferenceCount() {
        return totalReferenceCount;
    }

    public List<FileReference> getReferences() {
        return references;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileReferences that = (FileReferences) o;
        return totalReferenceCount == that.totalReferenceCount && Objects.equals(filename, that.filename) && Objects.equals(lastUpdateTime, that.lastUpdateTime) && Objects.equals(references, that.references);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, lastUpdateTime, totalReferenceCount, references);
    }

    @Override
    public String toString() {
        return "FileReferences{" +
                "filename='" + filename + '\'' +
                ", lastUpdateTime=" + lastUpdateTime +
                ", totalReferenceCount=" + totalReferenceCount +
                ", references=" + references +
                '}';
    }

    public static final class Builder {
        private String filename;
        private Instant lastUpdateTime;
        private int totalReferenceCount;
        private List<FileReference> references;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder filename(String filename) {
            this.filename = filename;
            return this;
        }

        public Builder lastUpdateTime(Instant lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
            return this;
        }

        public Builder totalReferenceCount(int totalReferenceCount) {
            this.totalReferenceCount = totalReferenceCount;
            return this;
        }

        public Builder references(List<FileReference> references) {
            this.references = references;
            return this;
        }

        public FileReferences build() {
            return new FileReferences(this);
        }
    }
}
