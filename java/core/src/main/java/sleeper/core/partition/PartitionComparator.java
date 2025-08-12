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
package sleeper.core.partition;

import sleeper.core.schema.Schema;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Comparator for organised paritions in correct orientation.
 */
public class PartitionComparator implements Comparator<Partition> {

    private Schema schema;

    PartitionComparator(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int compare(Partition partition1, Partition partition2) {
        String rowKey = schema.getRowKeyFields().get(0).getName();
        Object minA = partition1.getRegion().getRange(rowKey).getMin();
        Object minB = partition2.getRegion().getRange(rowKey).getMin();

        if (minA instanceof Long && minB instanceof Long) {
            return ((Long) minA).compareTo((Long) minB);
        }

        if (minA instanceof Integer && minB instanceof Integer) {
            return ((Integer) minA).compareTo((Integer) minB);
        }

        if (minA instanceof String && minB instanceof String) {
            return ((String) minA).compareTo((String) minB);
        }

        if (minA instanceof byte[] && minB instanceof byte[]) {
            return Arrays.compare((byte[]) minA, (byte[]) minB);
        }

        throw new PartitionTreeException("Unable to compare partitions due to range elements being an " +
                "unexpected class type. minA: " + minA.getClass() + ", minB: " + minB.getClass());
    }
}
