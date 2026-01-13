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

package sleeper.clients.report.filestatus;

import sleeper.core.statestore.FileReference;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Statistics on the numbers of file references per partition in a Sleeper table.
 */
public class FileReferencesStats {
    private final Integer minReferences;
    private final Integer maxReferences;
    private final Double averageReferences;
    private final Integer totalReferences;

    private FileReferencesStats(Integer minReferences, Integer maxReferences, Double averageReferences, Integer totalReferences) {
        this.minReferences = minReferences;
        this.maxReferences = maxReferences;
        this.averageReferences = averageReferences;
        this.totalReferences = totalReferences;
    }

    /**
     * Computes statistics on the numbers of file references per partition.
     *
     * @param  references the file references
     * @return            the statistics
     */
    public static FileReferencesStats from(Collection<FileReference> references) {
        Map<String, Set<String>> partitionIdToFiles = new TreeMap<>();
        references.forEach(reference -> {
            String partitionId = reference.getPartitionId();
            if (!partitionIdToFiles.containsKey(partitionId)) {
                partitionIdToFiles.put(partitionId, new HashSet<>());
            }
            partitionIdToFiles.get(partitionId).add(reference.getFilename());
        });
        Integer min = null;
        Integer max = null;
        int total = 0;
        int count = 0;
        for (Map.Entry<String, Set<String>> entry : partitionIdToFiles.entrySet()) {
            int size = entry.getValue().size();
            if (null == min) {
                min = size;
            } else if (size < min) {
                min = size;
            }
            if (null == max) {
                max = size;
            } else if (size > max) {
                max = size;
            }
            total += size;
            count++;
        }
        return new FileReferencesStats(min, max, average(total, count), references.size());
    }

    private static Double average(int total, int count) {
        if (count == 0) {
            return null;
        } else {
            return total / (double) count;
        }
    }

    public Integer getMinReferences() {
        return minReferences;
    }

    public Integer getMaxReferences() {
        return maxReferences;
    }

    public Double getAverageReferences() {
        return averageReferences;
    }

    public Integer getTotalReferences() {
        return totalReferences;
    }
}
