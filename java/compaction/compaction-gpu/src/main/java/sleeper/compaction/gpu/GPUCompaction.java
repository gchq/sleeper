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
package sleeper.compaction.gpu;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.gpu.ProtoCompaction.CompactionParams;
import sleeper.compaction.gpu.ProtoCompaction.CompactionResult;
import sleeper.compaction.gpu.ProtoCompaction.OptBytes;
import sleeper.compaction.gpu.ProtoCompaction.ReturnCode;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionRunner;
import sleeper.core.partition.Partition;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.range.Range;
import sleeper.core.range.Region;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.core.properties.table.TableProperty.COLUMN_INDEX_TRUNCATE_LENGTH;
import static sleeper.core.properties.table.TableProperty.COMPRESSION_CODEC;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS;
import static sleeper.core.properties.table.TableProperty.DICTIONARY_ENCODING_FOR_VALUE_FIELDS;
import static sleeper.core.properties.table.TableProperty.PAGE_SIZE;
import static sleeper.core.properties.table.TableProperty.PARQUET_WRITER_VERSION;
import static sleeper.core.properties.table.TableProperty.STATISTICS_TRUNCATE_LENGTH;

public class GPUCompaction implements CompactionRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(GPUCompaction.class);
    public static final int GRPC_PORT = 1430;

    /** Maximum number of rows in a Parquet row group. */
    private static final long GPU_MAX_ROW_GROUP_ROWS = 1_000_000;

    public GPUCompaction() {
    }

    protected ManagedChannel createChannel() {
        return ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).build();
    }

    @Override
    public RecordsProcessed compact(CompactionJob job, TableProperties tableProperties, Partition partition) throws IOException {
        Schema schema = tableProperties.getSchema();
        Region region = partition.getRegion();

        ManagedChannel channel = null;
        try {
            channel = createChannel();

            RecordsProcessed result = gRpcCompact(channel, job, tableProperties, schema, region);
            LOGGER.info("Compaction job {}: compaction finished at {}", job.getId(),
                    LocalDateTime.now());
            return result;
        } finally {
            if (channel != null) {
                try {
                    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    /**
     * Makes a gRPC call over the given channel to a compaction server.
     *
     * @param  channel         managed channel to server
     * @param  job             compaction job
     * @param  tableProperties table properties for compaction job
     * @param  schema          table schema
     * @param  region          table partition for compaction
     * @return                 number of records read and written in compaction
     * @throws IOException     if compaction failed, either due to a gRPC failure or a compaction failure
     */
    public RecordsProcessed gRpcCompact(Channel channel, CompactionJob job, TableProperties tableProperties, Schema schema, Region region) throws IOException {
        CompactorGrpc.CompactorBlockingStub stub = CompactorGrpc.newBlockingStub(channel)
                .withWaitForReady();
        CompactionParams params = createParams(job, tableProperties, schema, region);

        CompactionResult grpcResult;
        try {
            grpcResult = stub.compact(params);

            // check exit status of compaction
            if (grpcResult.getExitStatus() != ReturnCode.OK) {
                String exitReason = grpcResult.hasMsg() ? grpcResult.getMsg() : "not specified";
                throw new IOException("GPU compaction failed with exit status " + grpcResult.getExitStatus().name() + " due to " + exitReason);
            }

            RecordsProcessed result = new RecordsProcessed(grpcResult.getRowsRead(), grpcResult.getRowsWritten());
            return result;
        } catch (StatusRuntimeException e) {
            LOGGER.error("gRPC failed: {0}", e.getStatus());
            throw new IOException("GPU compaction failed due to gRPC failure: " + e.getStatus(), e);
        }
    }

    /**
     * Create the compaction message to send to the compaction server.
     *
     * @param  job             compaction job details
     * @param  tableProperties properties for the table being compacted
     * @param  schema          table schema
     * @param  region          compaction region
     * @return                 packaged compaction object
     */
    private CompactionParams createParams(CompactionJob job, TableProperties tableProperties, Schema schema, Region region) {
        @SuppressWarnings("unchecked")
        CompactionParams params = CompactionParams.newBuilder()
                .addAllInputFiles(job.getInputFiles())
                .setOutputFile(job.getOutputFile())
                .addAllRowKeyCols(schema.getRowKeyFieldNames())
                .addAllRowKeySchema(getKeyTypes(schema.getRowKeyTypes()))
                .addAllSortKeyCols(schema.getSortKeyFieldNames())
                .setMaxRowGroupSize(GPU_MAX_ROW_GROUP_ROWS)
                .setMaxPageSize(tableProperties.getInt(PAGE_SIZE))
                .setCompression(tableProperties.get(COMPRESSION_CODEC))
                .setWriterVersion(tableProperties.get(PARQUET_WRITER_VERSION))
                .setColumnTruncateLength(tableProperties.getInt(COLUMN_INDEX_TRUNCATE_LENGTH))
                .setStatsTruncateLength(tableProperties.getInt(STATISTICS_TRUNCATE_LENGTH))
                .setDictEncRowKeys(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_ROW_KEY_FIELDS))
                .setDictEncSortKeys(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_SORT_KEY_FIELDS))
                .setDictEncValues(tableProperties.getBoolean(DICTIONARY_ENCODING_FOR_VALUE_FIELDS))
                // Min array can't contain nulls
                .addAllRegionMins(encodeRegionBounds(region.getRanges().stream().map(Range::getMin), false))
                .addAllRegionMinsInclusive((Iterable<Boolean>) (region.getRanges().stream().map(Range::isMinInclusive).iterator()))
                .addAllRegionMaxs(encodeRegionBounds(region.getRanges().stream().map(Range::getMax), true))
                .addAllRegionMaxsInclusive((Iterable<Boolean>) (region.getRanges().stream().map(Range::isMaxInclusive).iterator()))
                .build();

        validate(params);
        return params;
    }

    /**
     * Validate state of compaction params.
     *
     * @throws IllegalStateException when a invariant fails
     */
    public static void validate(CompactionParams params) {
        // Check lengths
        long rowKeys = params.getRowKeyColsCount();
        if (rowKeys != params.getRowKeySchemaCount()) {
            throw new IllegalStateException("row key schema array has length " + params.getRowKeySchemaCount() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != params.getRegionMinsCount()) {
            throw new IllegalStateException("region mins has length " + params.getRegionMinsCount() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != params.getRegionMaxsCount()) {
            throw new IllegalStateException("region maxs has length " + params.getRegionMaxsCount() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != params.getRegionMinsInclusiveCount()) {
            throw new IllegalStateException("region mins inclusives has length " + params.getRegionMinsInclusiveCount() + " but there are " + rowKeys + " row key columns");
        }
        if (rowKeys != params.getRegionMaxsInclusiveCount()) {
            throw new IllegalStateException("region maxs inclusives has length " + params.getRegionMaxsInclusiveCount() + " but there are " + rowKeys + " row key columns");
        }
    }

    /**
     * Generate an iterable of bounds in the form of bytes.
     *
     * This takes a stream of range bounds and encodes them into bytes for gRPC transport. If nulls
     * are not allowed then any found will raise an Exception.
     *
     * @param  bounds               the list of bounds to encode
     * @param  nullsAllowed         if bounds are optional, that is if a value may be null
     * @return                      byte array of region bounds
     * @throws NullPointerException if a null is found but not allowed
     */
    @SuppressWarnings("unchecked")
    public static Iterable<OptBytes> encodeRegionBounds(Stream<Object> bounds, boolean nullsAllowed) {
        Stream<OptBytes> stream = bounds.map(item -> GPUCompaction.toOptBytes(item, nullsAllowed));
        return (Iterable<OptBytes>) stream;
    }

    /**
     * Converts a single item into a optional byte array.
     *
     * If nulls are allowed then item maybe null, otherwise an exception is raised. Number
     * types are encoded little endian byte order.
     *
     * @param  item                 the item to encode
     * @param  nullsAllowed         if nulls are allowed
     * @return                      an optional byte type
     * @throws NullPointerException if item is null and nullsAllowed is false
     * @throws ClassCastException   if item is not one of the permitted Sleeper row key types
     */
    public static OptBytes toOptBytes(Object item, boolean nullsAllowed) {
        if (item == null) {
            if (nullsAllowed) {
                return OptBytes.newBuilder().setNull(OptBytes.newBuilder().getNullBuilder()).build();
            } else {
                throw new NullPointerException("Illegal null bound found in compaction region");
            }
        } else if (item instanceof Integer) {
            int e = (int) item;
            ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(e);
            return OptBytes.newBuilder().setValue(ByteString.copyFrom(buf)).build();
        } else if (item instanceof Long) {
            long e = (long) item;
            ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(e);
            return OptBytes.newBuilder().setValue(ByteString.copyFrom(buf)).build();
        } else if (item instanceof java.lang.String) {
            // Strings are encoded as 4 byte length then value
            java.lang.String e = (java.lang.String) item;
            return OptBytes.newBuilder().setValue(ByteString.copyFromUtf8(e)).build();
        } else if (item instanceof byte[]) {
            byte[] e = (byte[]) item;
            return OptBytes.newBuilder().setValue(ByteString.copyFrom(e)).build();
        } else {
            throw new ClassCastException("Can't cast " + item.getClass() + " to a valid Sleeper row key type");
        }
    }

    /**
     * Convert a list of Sleeper primitive types to a number indicating their type
     * for FFI translation.
     *
     * @param  keyTypes              list of primitive types of columns
     * @return                       array of type IDs
     * @throws IllegalStateException if unsupported type found
     */
    public static List<Integer> getKeyTypes(List<PrimitiveType> keyTypes) {
        return keyTypes.stream().mapToInt(type -> {
            if (type instanceof IntType) {
                return 1;
            } else if (type instanceof LongType) {
                return 2;
            } else if (type instanceof StringType) {
                return 3;
            } else if (type instanceof ByteArrayType) {
                return 4;
            } else {
                throw new IllegalStateException("Unsupported column type found " + type.getClass());
            }
        }).boxed().collect(Collectors.toList());
    }

    @Override
    public String implementationLanguage() {
        return "C++";
    }

    @Override
    public boolean isHardwareAccelerated() {
        return true;
    }

    @Override
    public boolean supportsIterators() {
        return false;
    }
}
