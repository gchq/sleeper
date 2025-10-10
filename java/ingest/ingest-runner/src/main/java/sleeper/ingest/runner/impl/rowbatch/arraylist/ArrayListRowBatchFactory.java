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

package sleeper.ingest.runner.impl.rowbatch.arraylist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.row.Row;
import sleeper.core.rowbatch.RowBatch;
import sleeper.core.rowbatch.RowBatchFactory;
import sleeper.ingest.runner.impl.ParquetConfiguration;

import java.util.Objects;

import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_IN_MEMORY_BATCH_SIZE;
import static sleeper.core.properties.instance.ArrayListIngestProperty.MAX_ROWS_TO_WRITE_LOCALLY;

public class ArrayListRowBatchFactory<INCOMINGDATATYPE> implements RowBatchFactory<INCOMINGDATATYPE> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ArrayListRowBatchFactory.class);

    private final ParquetConfiguration parquetConfiguration;
    private final String localWorkingDirectory;
    private final int maxNoOfRowsInMemory;
    private final long maxNoOfRowsInLocalStore;
    private final ArrayListRowMapper<INCOMINGDATATYPE> rowMapper;

    private ArrayListRowBatchFactory(Builder<INCOMINGDATATYPE> builder) {
        parquetConfiguration = Objects.requireNonNull(builder.parquetConfiguration, "parquetConfiguration must not be null");
        localWorkingDirectory = Objects.requireNonNull(builder.localWorkingDirectory, "localWorkingDirectory must not be null");
        maxNoOfRowsInMemory = builder.maxNoOfRowsInMemory;
        maxNoOfRowsInLocalStore = builder.maxNoOfRowsInLocalStore;
        if (maxNoOfRowsInMemory < 1) {
            throw new IllegalArgumentException("maxNoOfRowsInMemory must be positive");
        }
        if (maxNoOfRowsInLocalStore < 1) {
            throw new IllegalArgumentException("maxNoOfRowsInLocalStore must be positive");
        }
        this.rowMapper = Objects.requireNonNull(builder.rowMapper, "rowMapper must not be null");

        LOGGER.info("Max number of rows to read into memory is {}", maxNoOfRowsInMemory);
        LOGGER.info("Max number of rows to write to local disk is {}", maxNoOfRowsInLocalStore);
    }

    public static Builder<?> builder() {
        return new Builder();
    }

    public static Builder<?> builderWith(InstanceProperties instanceProperties) {
        return builder().instanceProperties(instanceProperties);
    }

    @Override
    public RowBatch<INCOMINGDATATYPE> createRowBatch() {
        return new ArrayListRowBatch<>(
                parquetConfiguration, rowMapper, localWorkingDirectory,
                maxNoOfRowsInMemory, maxNoOfRowsInLocalStore);
    }

    public static final class Builder<T> {
        private ParquetConfiguration parquetConfiguration;
        private String localWorkingDirectory;
        private int maxNoOfRowsInMemory;
        private long maxNoOfRowsInLocalStore;
        private ArrayListRowMapper<T> rowMapper;

        private Builder() {
        }

        public Builder<T> parquetConfiguration(ParquetConfiguration parquetConfiguration) {
            this.parquetConfiguration = parquetConfiguration;
            return this;
        }

        public Builder<T> localWorkingDirectory(String localWorkingDirectory) {
            this.localWorkingDirectory = localWorkingDirectory;
            return this;
        }

        public Builder<T> maxNoOfRowsInMemory(int maxNoOfRowsInMemory) {
            this.maxNoOfRowsInMemory = maxNoOfRowsInMemory;
            return this;
        }

        public Builder<T> maxNoOfRowsInLocalStore(long maxNoOfRowsInLocalStore) {
            this.maxNoOfRowsInLocalStore = maxNoOfRowsInLocalStore;
            return this;
        }

        public Builder<T> instanceProperties(InstanceProperties instanceProperties) {
            return maxNoOfRowsInMemory(instanceProperties.getInt(MAX_IN_MEMORY_BATCH_SIZE))
                    .maxNoOfRowsInLocalStore(instanceProperties.getLong(MAX_ROWS_TO_WRITE_LOCALLY));
        }

        public <INCOMINGDATATYPE> Builder<INCOMINGDATATYPE> rowMapper(ArrayListRowMapper<INCOMINGDATATYPE> rowMapper) {
            this.rowMapper = (ArrayListRowMapper<T>) rowMapper;
            return (Builder<INCOMINGDATATYPE>) this;
        }

        public ArrayListRowBatchFactory<Row> buildAcceptingRows() {
            return rowMapper((ArrayListRowMapper<Row>) data -> data).build();
        }

        public ArrayListRowBatchFactory<T> build() {
            return new ArrayListRowBatchFactory<>(this);
        }
    }
}
