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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collector;

/**
 * Statistics on the numbers of file references per partition in a Sleeper table.
 */
public class FileReferencesStats {
    private final Integer minReferences;
    private final Integer maxReferences;
    private final Double meanReferences;
    private final Double medianReferences;
    private final Double percentile90;
    private final Double percentile99;
    private final Integer totalReferences;

    private FileReferencesStats(Integer minReferences, Integer maxReferences, Double meanReferences,
            Double medianReferences, Double percentile90, Double percentile99, Integer totalReferences) {
        this.minReferences = minReferences;
        this.maxReferences = maxReferences;
        this.meanReferences = meanReferences;
        this.medianReferences = medianReferences;
        this.percentile90 = percentile90;
        this.percentile99 = percentile99;
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
        Map<Integer, Integer> frequencyCounts = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : partitionIdToFiles.entrySet()) {
            int size = entry.getValue().size();
            frequencyCounts.merge(size, 1, Integer::sum);
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
        List<Integer> flatCounts = toFlatCounts(frequencyCounts);
        return new FileReferencesStats(min, max, mean(total, count), median(flatCounts),
                percentile(flatCounts, 0.90d), percentile(flatCounts, 0.99d), references.size());
    }

    private static Double percentile(List<Integer> flatCounts, double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("percentile must be in range [0, 1]: " + percentile);
        }
        if (flatCounts.isEmpty()) {
            return null;
        }
        double index = percentile * (flatCounts.size() - 1);
        int lowerIndex = (int) Math.floor(index);
        int upperIndex = (int) Math.ceil(index);
        if (lowerIndex == upperIndex) {
            return flatCounts.get(lowerIndex).doubleValue();
        }

        double lowerValue = flatCounts.get(lowerIndex);
        double upperValue = flatCounts.get(upperIndex);
        return lowerValue + (index - lowerIndex) * (upperValue - lowerValue);
    }

    private static Double mean(int total, int count) {
        if (count == 0) {
            return null;
        } else {
            return total / (double) count;
        }
    }

    private static List<Integer> toFlatCounts(Map<Integer, Integer> frequencyCounts) {
        List<Integer> flatCounts = frequencyCounts.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry<Integer, Integer>::getKey))
                .collect(Collector.of(ArrayList::new, (container, entry) -> {
                    for (int i = 0; i < entry.getValue(); i++) {
                        container.add(entry.getKey());
                    }
                }, (left, right) -> {
                    left.addAll(right);
                    return left;
                }, Collector.Characteristics.IDENTITY_FINISH));
        return flatCounts;
    }

    private static Double median(List<Integer> flatCounts) {
        if (flatCounts.isEmpty()) {
            return null;
        }
        int len = flatCounts.size();
        if (len % 2 == 1) {
            return Double.valueOf(flatCounts.get(len / 2));
        } else {
            return (flatCounts.get(len / 2 - 1) + flatCounts.get(len / 2)) / 2.0;
        }
    }

    public Integer getMinReferences() {
        return minReferences;
    }

    public Integer getMaxReferences() {
        return maxReferences;
    }

    public Double getMeanReferences() {
        return meanReferences;
    }

    public Double getMedianReferences() {
        return medianReferences;
    }

    /**
     * 90th percentile value from datasketches.
     *
     * @return 90th percentile
     */
    public Double get90thPercentile() {
        return percentile90;
    }

    /**
     * 99th percentile value from datasketches.
     *
     * @return 99th percentile
     */
    public Double get99thPercentile() {
        return percentile99;
    }

    public Integer getTotalReferences() {
        return totalReferences;
    }
}
