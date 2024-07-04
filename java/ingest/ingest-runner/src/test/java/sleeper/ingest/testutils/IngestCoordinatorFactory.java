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

package sleeper.ingest.testutils;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.record.Record;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.ingest.impl.partitionfilewriter.DirectPartitionFileWriterFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordBatchFactory;
import sleeper.ingest.impl.recordbatch.arrow.ArrowRecordWriter;

import java.util.function.Consumer;

import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.configuration.properties.instance.DefaultProperty.DEFAULT_INGEST_RECORD_BATCH_TYPE;
import static sleeper.configuration.properties.table.TablePropertiesTestHelper.createTestTablePropertiesWithNoSchema;
import static sleeper.ingest.testutils.IngestCoordinatorTestHelper.parquetConfiguration;

public class IngestCoordinatorFactory {

    private IngestCoordinatorFactory() {
    }

    public static IngestCoordinator<Record> ingestCoordinatorLocalDirectWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters) {
        return parameters.toBuilder().localDirectWrite().backedByArrow().buildCoordinator();
    }

    public static IngestCoordinator<Record> ingestCoordinatorS3DirectWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters) {
        return parameters.toBuilder().s3DirectWrite().backedByArrow().buildCoordinator();
    }

    public static IngestCoordinator<Record> ingestCoordinatorAsyncWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters) {
        return parameters.toBuilder().s3AsyncWrite().backedByArrow().buildCoordinator();
    }

    public static IngestCoordinator<Record> ingestCoordinatorLocalDirectWriteBackedByArrayList(
            IngestCoordinatorTestParameters parameters) {
        return parameters.toBuilder().localDirectWrite().backedByArrayList().buildCoordinator();
    }

    public static IngestCoordinator<Record> ingestCoordinatorS3DirectWriteBackedByArrayList(
            IngestCoordinatorTestParameters parameters) {
        return parameters.toBuilder().s3DirectWrite().backedByArrayList().buildCoordinator();
    }

    public static <T extends ArrowRecordWriter<U>, U> IngestCoordinator<U> ingestCoordinatorDirectWriteBackedByArrow(
            IngestCoordinatorTestParameters parameters, String filePathPrefix,
            Consumer<ArrowRecordBatchFactory.Builder<U>> arrowConfig,
            T recordWriter) {
        InstanceProperties instanceProperties = createTestInstanceProperties();
        instanceProperties.set(DEFAULT_INGEST_RECORD_BATCH_TYPE, "arraylist");
        instanceProperties.set(DEFAULT_INGEST_PARTITION_FILE_WRITER_TYPE, "direct");
        TableProperties tableProperties = createTestTablePropertiesWithNoSchema(instanceProperties);
        ArrowRecordBatchFactory.Builder<U> arrowConfigBuilder = ArrowRecordBatchFactory.builderWith(instanceProperties)
                .schema(parameters.getSchema())
                .localWorkingDirectory(parameters.getWorkingDir())
                .recordWriter(recordWriter);
        arrowConfig.accept(arrowConfigBuilder);
        return parameters.ingestCoordinatorBuilder(instanceProperties, tableProperties)
                .recordBatchFactory(arrowConfigBuilder.build())
                .partitionFileWriterFactory(DirectPartitionFileWriterFactory.from(
                        parquetConfiguration(parameters), filePathPrefix,
                        parameters.getFileNameGenerator()))
                .build();
    }
}
