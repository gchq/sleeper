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
package sleeper.compaction.jobexecution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.store.StoreUtils;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.stream.LongStream;

public class RustCompaction {
    /** Maximum number of rows in a Parquet row group. */
    private static final long RUST_MAX_ROW_GROUP_ROWS = 1_000_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(RustCompaction.class);

    private RustCompaction() {
    }

    public static RecordsProcessed compact(Schema schema, TableProperties tableProperties,
            CompactionJob compactionJob, StateStore stateStore) throws IOException {
        // Obtain native library. This throws an exception if native library can't be loaded and
        // linked
        RustBridge.Compaction nativeLib = RustBridge.getRustCompactor();

        // Figure out column numbers for row key fields
        // Row keys are numbered from zero, sort keys follow that
        long[] rowKeys = LongStream.rangeClosed(0, schema.getRowKeyFields().size()).toArray();
        long[] sortKeys = LongStream
                .rangeClosed(0, schema.getRowKeyFields().size() + schema.getSortKeyFields().size())
                .toArray();

        // Get the page size from table properties
        long maxPageSize = tableProperties.getLong(TableProperty.PAGE_SIZE);

        // Create object to hold the result (in native memory)
        RustBridge.FFICompactionResult compactionData = nativeLib.allocate_result();

        LOGGER.info("Invoking native Rust compaction...");

        try {
            // Perform compaction
            int result = nativeLib.ffi_merge_sorted_files(
                    compactionJob.getInputFiles().toArray(new String[0]),
                    compactionJob.getInputFiles().size(), compactionJob.getOutputFile(),
                    RUST_MAX_ROW_GROUP_ROWS, maxPageSize, rowKeys, rowKeys.length, sortKeys,
                    sortKeys.length, compactionData);

            // Check result
            if (result != 0) {
                LOGGER.error("Rust compaction failed, return code: {}", result);
                throw new IOException("Rust compaction failed with return code " + result);
            }

            long totalNumberOfRecordsRead = compactionData.rows_read.get();
            long recordsWritten = compactionData.rows_written.get();

            LOGGER.info("Compaction job {}: Read {} records and wrote {} records",
                    compactionJob.getId(), totalNumberOfRecordsRead, recordsWritten);

            StoreUtils.updateStateStoreSuccess(compactionJob.getInputFiles(), compactionJob.getOutputFile(),
                    compactionJob.getPartitionId(), recordsWritten, stateStore);
            LOGGER.info("Compaction job {}: compaction finished at {}", compactionJob.getId(),
                    LocalDateTime.now());

            return new RecordsProcessed(totalNumberOfRecordsRead, recordsWritten);
        } finally {
            // Ensure de-allocation
            nativeLib.free_result(compactionData);
        }
    }
}
