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

    private final AllFileReferences allFileReferences;
    private final List<Partition> partitions;

    private StateStoreSnapshot(Builder builder) {
        allFileReferences = builder.allFileReferences;
        partitions = builder.partitions;
    }

    public Stream<Partition> partitions() {
        return partitions.stream();
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Stream<FileInfo> active() {
        return allFileReferences.getActiveFiles().stream();
    }

    public int activeCount() {
        return allFileReferences.getActiveFiles().size();
    }

    public List<FileInfo> getActive() {
        return allFileReferences.getActiveFiles();
    }

    public List<String> getFilesWithNoReferences() {
        return allFileReferences.getFilesWithNoReferences();
    }

    public boolean isMoreThanMax() {
        return allFileReferences.isMoreThanMax();
    }

    public static StateStoreSnapshot from(StateStore stateStore, int maxNumberOfFilesWithNoReferencesToCount) throws StateStoreException {
        AllFileReferences allFileReferences = AllFileReferences.fromStateStoreWithReadyForGCLimit(stateStore, maxNumberOfFilesWithNoReferencesToCount);
        return builder()
                .allFileReferences(allFileReferences)
                .partitions(stateStore.getAllPartitions())
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<Partition> partitions;
        private AllFileReferences allFileReferences;

        private Builder() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public Builder allFileReferences(AllFileReferences allFileReferences) {
            this.allFileReferences = allFileReferences;
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
