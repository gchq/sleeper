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
package sleeper.ingest.impl.partitionfilewriter;

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.statestore.FileReference;

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
     * Create a reference to a new file to add to the state store. This should be passed to
     * {@link sleeper.core.statestore.StateStore.addFile}.
     *
     * @param  filename        the full path to the file, including file system
     * @param  partitionId     the ID of the partition the reference should be added to
     * @param  numberOfRecords the number of records in the file
     * @return                 the {@link FileReference} object
     */
    public static FileReference createFileReference(
            String filename, String partitionId, long numberOfRecords) {
        return FileReference.builder()
                .filename(filename)
                .partitionId(partitionId)
                .numberOfRecords(numberOfRecords)
                .countApproximate(false)
                .onlyContainsDataForThisPartition(true)
                .build();
    }

    /**
     * Create a map with an empty sketch for all row keys in a schema. This is to be used with
     * {@link #updateQuantileSketchMap} to create sketches for a file.
     *
     * @param  sleeperSchema The schema to create sketches for
     * @return               A map from each row key field name to an empty sketch
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
     * Updates sketches with a new record, for every row key in a schema. The map and sketches are updated in-place.
     * This is to be used with {@link #createQuantileSketchMap} to create sketches for a file.
     *
     * @param sleeperSchema       The schema to create sketches for
     * @param keyFieldToSketchMap A map from each row key field name to a sketch
     * @param record              The record to update each sketch with
     */
    public static void updateQuantileSketchMap(
            Schema sleeperSchema, Map<String, ItemsSketch> keyFieldToSketchMap, Record record) {
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
