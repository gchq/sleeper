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
package sleeper.clients.status.report.filestatus;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.AllFileReferences;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;
import sleeper.core.statestore.StateStoreException;

import java.util.List;
import java.util.stream.Stream;

public class StateStoreSnapshot {

    private final List<FileInfo> active;
    private final StateStoreFilesWithNoReferences filesWithNoReferences;
    private final List<Partition> partitions;

    private StateStoreSnapshot(Builder builder) {
        active = builder.active;
        filesWithNoReferences = builder.filesWithNoReferences;
        partitions = builder.partitions;
    }

    public Stream<Partition> partitions() {
        return partitions.stream();
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Stream<FileInfo> active() {
        return active.stream();
    }

    public int activeCount() {
        return active.size();
    }

    public List<FileInfo> getActive() {
        return active;
    }

    public StateStoreFilesWithNoReferences getFilesWithNoReferences() {
        return filesWithNoReferences;
    }

    public static StateStoreSnapshot from(StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount) throws StateStoreException {
        AllFileReferences allFileReferences = stateStore.getAllFileReferences();
        return builder()
                .active(allFileReferences.getActiveFiles())
                .filesWithNoReferences(StateStoreFilesWithNoReferences.from(allFileReferences.getFilesWithNoReferences(),
                        maxNumberOfFilesWithNoReferencesToCount))
                .partitions(stateStore.getAllPartitions())
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<FileInfo> active;
        private StateStoreFilesWithNoReferences filesWithNoReferences;
        private List<Partition> partitions;

        private Builder() {
        }

        public Builder active(List<FileInfo> active) {
            this.active = active;
            return this;
        }

        public Builder filesWithNoReferences(StateStoreFilesWithNoReferences filesWithNoReferences) {
            this.filesWithNoReferences = filesWithNoReferences;
            return this;
        }

        public Builder partitions(List<Partition> partitions) {
            this.partitions = partitions;
            return this;
        }

        public StateStoreSnapshot build() {
            return new StateStoreSnapshot(this);
        }
    }
}
