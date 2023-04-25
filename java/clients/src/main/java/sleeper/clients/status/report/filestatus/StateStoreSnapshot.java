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
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.util.List;
import java.util.stream.Stream;

public class StateStoreSnapshot {

    private final List<FileInfo> active;
    private final StateStoreReadyForGC readyForGC;
    private final List<Partition> partitions;

    private StateStoreSnapshot(Builder builder) {
        active = builder.active;
        readyForGC = builder.readyForGC;
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

    public StateStoreReadyForGC getReadyForGC() {
        return readyForGC;
    }

    public static StateStoreSnapshot from(StateStore stateStore, int maxNumberOfReadyForGCFilesToCount) throws StateStoreException {
        return builder()
                .active(stateStore.getActiveFiles())
                .readyForGC(StateStoreReadyForGC.from(stateStore, maxNumberOfReadyForGCFilesToCount))
                .partitions(stateStore.getAllPartitions())
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<FileInfo> active;
        private StateStoreReadyForGC readyForGC;
        private List<Partition> partitions;

        private Builder() {
        }

        public Builder active(List<FileInfo> active) {
            this.active = active;
            return this;
        }

        public Builder readyForGC(StateStoreReadyForGC readyForGC) {
            this.readyForGC = readyForGC;
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
