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

package sleeper.ingest.testutils;

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.StandardIngestCoordinator;
import sleeper.ingest.impl.partitionfilewriter.PartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;
import sleeper.statestore.StateStore;

public class IngestCoordinatorTestHelper {
    public static ParquetConfiguration parquetConfiguration(Schema schema, Configuration hadoopConfiguration) {
        return ParquetConfiguration.builder()
                .parquetCompressionCodec("zstd")
                .sleeperSchema(schema)
                .hadoopConfiguration(hadoopConfiguration)
                .build();
    }

    public static IngestCoordinator<Record> standardIngestCoordinator(
            StateStore stateStore, Schema schema,
            RecordBatchFactory<Record> recordBatchFactory, PartitionFileWriterFactory partitionFileWriterFactory) {
        return StandardIngestCoordinator.builder()
                .objectFactory(ObjectFactory.noUserJars())
                .ingestPartitionRefreshFrequencyInSeconds(Integer.MAX_VALUE)
                .stateStore(stateStore)
                .schema(schema)
                .recordBatchFactory(recordBatchFactory)
                .partitionFileWriterFactory(partitionFileWriterFactory)
                .build();
    }
}
