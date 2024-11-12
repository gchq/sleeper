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
package sleeper.systemtest.dsl.snapshot;

import sleeper.core.partition.PartitionTree;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.AllReferencesToAllFiles;

import java.util.Optional;

import static sleeper.core.properties.instance.CommonProperty.ID;

public interface SnapshotsDriver {
    void enableCreation(InstanceProperties instanceProperties);

    void disableCreation(InstanceProperties instanceProperties);

    default Optional<AllReferencesToAllFiles> loadLatestFilesSnapshot(InstanceProperties instanceProperties, TableProperties tableProperties) {
        throw new UnsupportedOperationException(
                "Requested loading files snapshot for instance " + instanceProperties.get(ID) +
                        ", table " + tableProperties.getStatus() + ", not currently implemented for this driver");
    }

    default Optional<PartitionTree> loadLatestPartitionsSnapshot(InstanceProperties instanceProperties, TableProperties tableProperties) {
        throw new UnsupportedOperationException(
                "Requested loading partitions snapshot for instance " + instanceProperties.get(ID) +
                        ", table " + tableProperties.getStatus() + ", not currently implemented for this driver");
    }
}
