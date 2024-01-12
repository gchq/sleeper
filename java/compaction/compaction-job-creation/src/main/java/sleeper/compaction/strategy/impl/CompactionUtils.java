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
package sleeper.compaction.strategy.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.partition.Partition;
import sleeper.core.statestore.FileReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class CompactionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionUtils.class);

    private CompactionUtils() {
    }

    public static List<FileReference> getFilesInAscendingOrder(String tableName, Partition partition, List<FileReference> fileReferences) {
        // Get files in this partition
        List<FileReference> files = fileReferences
                .stream()
                .filter(f -> f.getPartitionId().equals(partition.getId()))
                .collect(Collectors.toList());
        LOGGER.info("Creating jobs for leaf partition {} in table {} (there are {} files for this partition)", partition.getId(), tableName, files.size());

        // Create map of number of records in file to files, sorted by number of records in file
        SortedMap<Long, List<FileReference>> recordsToFiles = new TreeMap<>();
        for (FileReference fileReference : files) {
            if (!recordsToFiles.containsKey(fileReference.getNumberOfRecords())) {
                recordsToFiles.put(fileReference.getNumberOfRecords(), new ArrayList<>());
            }
            recordsToFiles.get(fileReference.getNumberOfRecords()).add(fileReference);
        }

        // Convert to list of FileInfos in ascending order of number of records
        List<FileReference> fileInfosList = new ArrayList<>();
        for (Map.Entry<Long, List<FileReference>> entry : recordsToFiles.entrySet()) {
            fileInfosList.addAll(entry.getValue());
        }

        return fileInfosList;
    }
}
