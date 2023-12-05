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
package sleeper.ingest.impl.partitionfilewriter;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.StateStore;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * A utility class providing static functions that are useful when wrtiting partition files.
 */
public class PartitionFileWriterUtils {
    /**
     * This class should not be instantiated.
     */
    private PartitionFileWriterUtils() {
    }

    /**
     * Create a {@link FileInfo} object to use to add the file to a {@link StateStore}
     *
     * @param filename        -
     * @param partitionId     -
     * @param numberOfRecords -
     * @return The {@link FileInfo} object
     */
    public static FileInfo createFileInfo(String filename,
                                          String partitionId,
                                          long numberOfRecords) {
        return FileInfo.wholeFile()
                .filename(filename)
                .partitionId(partitionId)
                .fileStatus(FileInfo.FileStatus.ACTIVE)
                .numberOfRecords(numberOfRecords)
                .build();
    }

    /**
     * Create a Map from field name to an {@link ItemsSketch}, for every field in the supplied schema
     *
     * @param sleeperSchema -
     * @return The map
     */
    public static Map<String, ItemsSketch> createQuantileSketchMap(Schema sleeperSchema) {
        Map<String, ItemsSketch> keyFieldToSketch = new HashMap<>();
        sleeperSchema.getRowKeyFields().forEach(rowKeyField -> {
            ItemsSketch sketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
            keyFieldToSketch.put(rowKeyField.getName(), sketch);
        });
        return keyFieldToSketch;
    }

    /**
     * Update every {@link ItemsSketch} in the supplied map with the corresponding fields in the supplied {@link
     * Record}. The map and sketches are updated in-place.
     *
     * @param sleeperSchema       -
     * @param keyFieldToSketchMap The map to update
     * @param record              The record to update with
     */
    public static void updateQuantileSketchMap(Schema sleeperSchema,
                                               Map<String, ItemsSketch> keyFieldToSketchMap,
                                               Record record) {
        for (Field rowKeyField : sleeperSchema.getRowKeyFields()) {
            if (rowKeyField.getType() instanceof ByteArrayType) {
                byte[] value = (byte[]) record.get(rowKeyField.getName());
                keyFieldToSketchMap.get(rowKeyField.getName()).update(ByteArray.wrap(value));
            } else {
                Object value = record.get(rowKeyField.getName());
                keyFieldToSketchMap.get(rowKeyField.getName()).update(value);
            }
        }
    }
}
