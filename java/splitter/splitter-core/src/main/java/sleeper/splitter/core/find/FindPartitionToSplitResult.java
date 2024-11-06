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

package sleeper.splitter.core.find;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;

import java.util.List;

public class FindPartitionToSplitResult {
    private final String tableId;
    private final Partition partition;
    private final List<FileReference> relevantFiles;

    public FindPartitionToSplitResult(String tableId, Partition partition, List<FileReference> relevantFiles) {
        this.tableId = tableId;
        this.partition = partition;
        this.relevantFiles = relevantFiles;
    }

    public String getTableId() {
        return tableId;
    }

    public Partition getPartition() {
        return partition;
    }

    public List<FileReference> getRelevantFiles() {
        return relevantFiles;
    }
}
