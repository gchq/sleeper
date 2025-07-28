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
package sleeper.cdk.stack.core;

import java.util.stream.Stream;

public class StateStoreGrants {

    private final Access fileReferences;
    private final Access unreferencedFiles;
    private final Access partitions;

    public static Builder builder() {
        return new Builder();
    }

    public static StateStoreGrants readPartitions() {
        return builder().partitions(Access.READ).build();
    }

    public static StateStoreGrants readWritePartitions() {
        return builder().partitions(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readPartitionsReadWriteFileReferences() {
        return builder().partitions(Access.READ).fileReferences(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readFileReferencesReadWritePartitions() {
        return builder().fileReferences(Access.READ).partitions(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readWriteAllFilesAndPartitions() {
        return builder().fileReferences(Access.READ_WRITE).unreferencedFiles(Access.READ_WRITE).partitions(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readFileReferencesAndPartitions() {
        return builder().fileReferences(Access.READ).partitions(Access.READ).build();
    }

    public static StateStoreGrants readAllFilesAndPartitions() {
        return builder().fileReferences(Access.READ).unreferencedFiles(Access.READ).partitions(Access.READ).build();
    }

    public static StateStoreGrants readWriteUnreferencedFiles() {
        return builder().unreferencedFiles(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readWriteFileReferencesAndUnreferenced() {
        return builder().fileReferences(Access.READ_WRITE).unreferencedFiles(Access.READ_WRITE).build();
    }

    private StateStoreGrants(Builder builder) {
        this.fileReferences = builder.fileReferences;
        this.unreferencedFiles = builder.unreferencedFiles;
        this.partitions = builder.partitions;
    }

    public boolean canWriteFileReferences() {
        return fileReferences.canWrite();
    }

    public boolean canReadFileReferences() {
        return fileReferences.canRead();
    }

    public boolean canReadFileReferencesOrUnreferencedFiles() {
        return fileReferences.canRead() || unreferencedFiles.canRead();
    }

    public boolean canWriteFileReferencesOrUnreferencedFiles() {
        return fileReferences.canWrite() || unreferencedFiles.canWrite();
    }

    public boolean canWritePartitions() {
        return partitions.canWrite();
    }

    public boolean canReadPartitions() {
        return partitions.canRead();
    }

    public boolean canReadAny() {
        return allData().anyMatch(Access::canRead);
    }

    public boolean canWriteAny() {
        return allData().anyMatch(Access::canWrite);
    }

    private Stream<Access> allData() {
        return Stream.of(fileReferences, unreferencedFiles, partitions);
    }

    public static class Builder {
        private Access fileReferences = Access.NO_ACCESS;
        private Access unreferencedFiles = Access.NO_ACCESS;
        private Access partitions = Access.NO_ACCESS;

        public Builder fileReferences(Access fileReferences) {
            this.fileReferences = fileReferences;
            return this;
        }

        public Builder unreferencedFiles(Access unreferencedFiles) {
            this.unreferencedFiles = unreferencedFiles;
            return this;
        }

        public Builder partitions(Access partitions) {
            this.partitions = partitions;
            return this;
        }

        public StateStoreGrants build() {
            return new StateStoreGrants(this);
        }
    }

    public enum Access {
        NO_ACCESS(false, false),
        READ(true, false),
        READ_WRITE(true, true);

        private final boolean canRead;
        private final boolean canWrite;

        Access(boolean canRead, boolean canWrite) {
            this.canRead = canRead;
            this.canWrite = canWrite;
        }

        public boolean canRead() {
            return canRead;
        }

        public boolean canWrite() {
            return canWrite;
        }
    }
}
