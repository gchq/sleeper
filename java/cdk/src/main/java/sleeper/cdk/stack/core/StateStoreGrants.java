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
package sleeper.cdk.stack.core;

import java.util.stream.Stream;

public class StateStoreGrants {

    private final Access activeFiles;
    private final Access readyForGCFiles;
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

    public static StateStoreGrants readPartitionsReadWriteActiveFiles() {
        return builder().partitions(Access.READ).activeFiles(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readActiveFilesReadWritePartitions() {
        return builder().activeFiles(Access.READ).partitions(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readWriteAllFilesAndPartitions() {
        return builder().activeFiles(Access.READ_WRITE).readyForGCFiles(Access.READ_WRITE).partitions(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readActiveFilesAndPartitions() {
        return builder().activeFiles(Access.READ).partitions(Access.READ).build();
    }

    public static StateStoreGrants readAllFilesAndPartitions() {
        return builder().activeFiles(Access.READ).readyForGCFiles(Access.READ).partitions(Access.READ).build();
    }

    public static StateStoreGrants readWriteReadyForGCFiles() {
        return builder().readyForGCFiles(Access.READ_WRITE).build();
    }

    public static StateStoreGrants readWriteActiveAndReadyForGCFiles() {
        return builder().activeFiles(Access.READ_WRITE).readyForGCFiles(Access.READ_WRITE).build();
    }

    private StateStoreGrants(Builder builder) {
        this.activeFiles = builder.activeFiles;
        this.readyForGCFiles = builder.readyForGCFiles;
        this.partitions = builder.partitions;
    }

    public boolean canWriteActiveFiles() {
        return activeFiles.canWrite();
    }

    public boolean canReadActiveFiles() {
        return activeFiles.canRead();
    }

    public boolean canReadActiveOrReadyForGCFiles() {
        return activeFiles.canRead() || readyForGCFiles.canRead();
    }

    public boolean canWriteActiveOrReadyForGCFiles() {
        return activeFiles.canWrite() || readyForGCFiles.canWrite();
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
        return Stream.of(activeFiles, readyForGCFiles, partitions);
    }

    public static class Builder {
        private Access activeFiles = Access.NO_ACCESS;
        private Access readyForGCFiles = Access.NO_ACCESS;
        private Access partitions = Access.NO_ACCESS;

        public Builder activeFiles(Access activeFiles) {
            this.activeFiles = activeFiles;
            return this;
        }

        public Builder readyForGCFiles(Access readyForGCFiles) {
            this.readyForGCFiles = readyForGCFiles;
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
