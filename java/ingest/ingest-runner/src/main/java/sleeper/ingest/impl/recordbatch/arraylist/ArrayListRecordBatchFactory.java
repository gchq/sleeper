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

package sleeper.ingest.impl.recordbatch.arraylist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.Record;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;

import java.util.Objects;
import java.util.function.Function;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.MAX_RECORDS_TO_WRITE_LOCALLY;

public class ArrayListRecordBatchFactory<INCOMINGDATATYPE> implements RecordBatchFactory<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayListRecordBatchFactory.class);

    private final ParquetConfiguration parquetConfiguration;
    private final String localWorkingDirectory;
    private final int maxNoOfRecordsInMemory;
    private final long maxNoOfRecordsInLocalStore;
    private final Function<ArrayListRecordBatchFactory<?>, RecordBatch<INCOMINGDATATYPE>> createBatchFn;

    private ArrayListRecordBatchFactory(Builder builder,
                                        Function<ArrayListRecordBatchFactory<?>, RecordBatch<INCOMINGDATATYPE>> createBatchFn) {
        parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetConfiguration must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        maxNoOfRecordsInMemory = builder.maxNoOfRecordsInMemory;
        maxNoOfRecordsInLocalStore = builder.maxNoOfRecordsInLocalStore;
        if (maxNoOfRecordsInMemory < 1) {
            throw new IllegalArgumentException("maxNoOfRecordsInMemory must be positive");
        }
        if (maxNoOfRecordsInLocalStore < 1) {
            throw new IllegalArgumentException("maxNoOfRecordsInLocalStore must be positive");
        }
        this.createBatchFn = Objects.requireNonNull(createBatchFn, "createBatchFn must not be null");

        LOGGER.info("Max number of records to read into memory is {}", maxNoOfRecordsInMemory);
        LOGGER.info("Max number of records to write to local disk is {}", maxNoOfRecordsInLocalStore);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderWith(InstanceProperties instanceProperties) {
        return builder().instanceProperties(instanceProperties);
    }

    public ArrayListRecordBatchAcceptingRecords createRecordBatchAcceptingRecords() {
        return new ArrayListRecordBatchAcceptingRecords(
                parquetConfiguration,
                localWorkingDirectory,
                maxNoOfRecordsInMemory,
                maxNoOfRecordsInLocalStore);
    }

    @Override
    public RecordBatch<INCOMINGDATATYPE> createRecordBatch() {
        return createBatchFn.apply(this);
    }

    public static final class Builder {
        private ParquetConfiguration parquetConfiguration;
        private String localWorkingDirectory;
        private int maxNoOfRecordsInMemory;
        private long maxNoOfRecordsInLocalStore;

        private Builder() {
        }

        public Builder parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
            return this;
        }

        public Builder localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder maxNoOfRecordsInMemory(int maxNoOfRecordsInMemory) {
            this.maxNoOfRecordsInMemory = maxNoOfRecordsInMemory;
            return this;
        }

        public Builder maxNoOfRecordsInLocalStore(long maxNoOfRecordsInLocalStore) {
            this.maxNoOfRecordsInLocalStore = maxNoOfRecordsInLocalStore;
            return this;
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            return maxNoOfRecordsInMemory(instanceProperties.getInt(MAX_IN_MEMORY_BATCH_SIZE))
                    .maxNoOfRecordsInLocalStore(instanceProperties.getLong(MAX_RECORDS_TO_WRITE_LOCALLY));
        }

        public ArrayListRecordBatchFactory<Record> buildAcceptingRecords() {
            return new ArrayListRecordBatchFactory<>(this, ArrayListRecordBatchFactory::createRecordBatchAcceptingRecords);
        }
    }
}
