/*
 * Copyright 2022 Crown Copyright
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import sleeper.core.key.Key;
import sleeper.core.partition.Partition;
import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.io.parquet.record.ParquetRecordWriter;
import sleeper.io.parquet.record.SchemaConverter;
import sleeper.statestore.FileInfo;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
     * Create a {@link FileInfo} object to use to add the file to a {@link sleeper.statestore.StateStore}
     *
     * @param sleeperSchema -
     * @param filename      -
     * @param partitionId   -
     * @param numberOfLines -
     * @param minKey        -
     * @param maxKey        -
     * @param updateTime    -
     * @return The {@link FileInfo} object
     */
    public static FileInfo createFileInfo(Schema sleeperSchema,
                                          String filename,
                                          String partitionId,
                                          long numberOfLines,
                                          Object minKey,
                                          Object maxKey,
                                          long updateTime) {
        FileInfo fileInfo = new FileInfo();
        fileInfo.setRowKeyTypes(sleeperSchema.getRowKeyTypes());
        fileInfo.setFilename(filename);
        fileInfo.setPartitionId(partitionId);
        fileInfo.setFileStatus(FileInfo.FileStatus.ACTIVE);
        fileInfo.setNumberOfRecords(numberOfLines);
        fileInfo.setMinRowKey(Key.create(minKey));
        fileInfo.setMaxRowKey(Key.create(maxKey));
        fileInfo.setLastStateStoreUpdateTime(updateTime);
        return fileInfo;
    }

    /**
     * Create a {@link ParquetWriter} for {@link Record} objects, based on the supplied details.
     *
     * @param outputFile              The file to write to, which may include a prefix such as s3a://
     * @param sleeperSchema           -
     * @param parquetCompressionCodec -
     * @param parquetRowGroupSize     -
     * @param parquetPageSize         -
     * @param hadoopConfiguration     The Hadoop configuration to use to create the Parquet writer. This allows the
     *                                library to locate classes which correspond to a prefix such as s3a://. Note that
     *                                the library uses a cache and so unusual errors may occur if this configuration
     *                                changes.
     * @return The {@link ParquetWriter}
     * @throws IOException -
     */
    public static ParquetWriter<Record> createParquetWriter(String outputFile,
                                                            Schema sleeperSchema,
                                                            String parquetCompressionCodec,
                                                            long parquetRowGroupSize,
                                                            int parquetPageSize,
                                                            Configuration hadoopConfiguration) throws IOException {
        ParquetRecordWriter.Builder builder = new ParquetRecordWriter.Builder(new Path(outputFile),
                SchemaConverter.getSchema(sleeperSchema), sleeperSchema)
                .withCompressionCodec(CompressionCodecName.fromConf(parquetCompressionCodec))
                .withRowGroupSize(parquetRowGroupSize)
                .withPageSize(parquetPageSize)
                .withConf(hadoopConfiguration);
        return builder.build();
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

    /**
     * Construct the filename for the Parquet partition file, to maintain consistency across different file writer
     * implementations.
     *
     * @param filePathPrefix -
     * @param partition      -
     * @param uuid           -
     * @return The file name
     */
    public static String constructPartitionParquetFileName(String filePathPrefix,
                                                           Partition partition,
                                                           UUID uuid) {
        return String.format("%s/partition_%s/%s.parquet", filePathPrefix, partition.getId(), uuid);
    }

    /**
     * Construct the filename for the quantile sketches file, to maintain consistency across different file writer
     * implementations.
     *
     * @param filePathPrefix -
     * @param partition      -
     * @param uuid           -
     * @return The file name
     */
    public static String constructQuantileSketchesFileName(String filePathPrefix,
                                                           Partition partition,
                                                           UUID uuid) {
        return String.format("%s/partition_%s/%s.sketches", filePathPrefix, partition.getId(), uuid);
    }
}
