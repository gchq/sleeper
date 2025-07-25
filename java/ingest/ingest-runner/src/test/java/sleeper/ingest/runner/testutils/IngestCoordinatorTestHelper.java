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

package sleeper.ingest.runner.testutils;

import org.apache.hadoop.conf.Configuration;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;
import sleeper.core.util.ObjectFactory;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.impl.ParquetConfiguration;
import sleeper.ingest.runner.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.runner.impl.rowbatch.RowBatchFactory;

import java.time.Instant;

import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;

public class IngestCoordinatorTestHelper {
    private IngestCoordinatorTestHelper() {
    }

    public static ParquetConfiguration parquetConfiguration(Schema schema, Configuration hadoopConfiguration) {
        TableProperties tableProperties = new TableProperties(new InstanceProperties());
        tableProperties.set(COMPRESSION_CODEC, "zstd");
        tableProperties.setSchema(schema);
        return ParquetConfiguration.builder()
                .tableProperties(tableProperties)
                .hadoopConfiguration(hadoopConfiguration)
                .build();
    }

    public static <T> IngestCoordinator<T> standardIngestCoordinator(
            StateStore stateStore, Schema schema,
            RowBatchFactory<T> recordBatchFactory, PartitionFileWriterFactory partitionFileWriterFactory) {
        return standardIngestCoordinatorBuilder(stateStore, schema, recordBatchFactory, partitionFileWriterFactory).build();
    }

    public static <T> IngestCoordinator.Builder<T> standardIngestCoordinatorBuilder(
            StateStore stateStore, Schema schema,
            RowBatchFactory<T> recordBatchFactory, PartitionFileWriterFactory partitionFileWriterFactory) {
        return IngestCoordinator.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .ingestPartitionRefreshFrequencyInSeconds(Integer.MAX_VALUE)
                .stateStore(stateStore)
                .schema(schema)
                .recordBatchFactory(recordBatchFactory)
                .partitionFileWriterFactory(partitionFileWriterFactory);
    }

    public static FileReference.Builder accurateFileReferenceBuilder(
            String filename, String partitionId, long numberOfRecords, Instant updateTime) {
        return FileReference.builder()
                .partitionId(partitionId)
                .filename(filename)
                .numberOfRows(numberOfRecords)
                .countApproximate(false)
                .lastStateStoreUpdateTime(updateTime);
    }

    public static FileReference accurateSplitFileReference(
            FileReference fileReference, String partitionId, long numberOfRecords, Instant updateTime) {
        return accurateSplitFileReference(fileReference.getFilename(), partitionId, numberOfRecords, updateTime);
    }

    public static FileReference accurateSplitFileReference(
            String filename, String partitionId, long numberOfRecords, Instant updateTime) {
        return accurateFileReferenceBuilder(filename, partitionId, numberOfRecords, updateTime)
                .onlyContainsDataForThisPartition(false)
                .build();
    }

}
