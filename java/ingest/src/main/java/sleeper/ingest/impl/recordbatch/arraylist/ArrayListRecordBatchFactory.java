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

package sleeper.ingest.impl.recordbatch.arraylist;

import sleeper.core.record.Record;
import sleeper.ingest.impl.ParquetConfiguration;
import sleeper.ingest.impl.recordbatch.RecordBatch;
import sleeper.ingest.impl.recordbatch.RecordBatchFactory;

import java.util.Objects;
import java.util.function.Function;

public class ArrayListRecordBatchFactory<INCOMINGDATATYPE> implements RecordBatchFactory<INCOMINGDATATYPE> {
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
    }

    public static Builder builder() {
        return new Builder();
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

        public ArrayListRecordBatchFactory<Record> buildAcceptingRecords() {
            return new ArrayListRecordBatchFactory<>(this, ArrayListRecordBatchFactory::createRecordBatchAcceptingRecords);
        }
    }
}
