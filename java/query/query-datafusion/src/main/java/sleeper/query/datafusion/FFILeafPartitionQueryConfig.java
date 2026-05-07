/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.query.datafusion;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;

import sleeper.foreign.FFIBytes;
import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.datafusion.FFICommonConfig;

/**
 * The leaf query data that will be populated from the Java side.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects/query.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
@SuppressFBWarnings(value = {"PA_PUBLIC_MUTABLE_OBJECT_ATTRIBUTE", "URF_UNREAD_FIELD"})
public class FFILeafPartitionQueryConfig extends Struct {
    /** Basic configuration for query. */
    public final Struct.StructRef<FFICommonConfig> common = new StructRef<>(FFICommonConfig.class);
    /** Prevents GC of pointee until this object is collected. */
    private FFICommonConfig java_common;
    /** Length of query region array. */
    public final Struct.size_t query_region_len = new Struct.size_t();
    /** The array of query regions. */
    public final Struct.StructRef<FFISleeperRegion> query_regions = new StructRef<>(FFISleeperRegion.class);
    /** Prevents GC of pointee until this object is collected. */
    private FFISleeperRegion[] java_query_regions;
    /** Specifies if there are any requested value fields. */
    public final Struct.Boolean requested_value_fields_set = new Struct.Boolean();
    /** Length of requested value fields array. */
    public final Struct.size_t requested_value_fields_len = new Struct.size_t();
    /** Requested value columns. */
    public final Struct.StructRef<FFIBytes> requested_value_fields = new Struct.StructRef<>(FFIBytes.class);
    /** Prevents GC of pointee until this object is collected. */
    private FFIBytes[] java_requested_value_fields;
    /** Specifies if logical and physical DataFusion query plans should be written to a log output. */
    public final Struct.Boolean explain_plans = new Struct.Boolean();

    public FFILeafPartitionQueryConfig(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Sets the common Sleeper configuration.
     *
     * @param config Sleeper config
     */
    public void setCommonConfig(FFICommonConfig config) {
        common.set(config);
        java_common = config;
    }

    /**
     * Sets query region field and length.
     *
     * @param regions query regions
     */
    public void setQueryRegions(FFISleeperRegion[] regions) {
        query_region_len.set(regions.length);
        query_regions.set(regions);
        java_query_regions = regions;
    }

    /**
     * Sets requested value fields and length.
     *
     * @param requestedValueFields array of fields
     */
    public void setRequestedValueFields(FFIBytes[] requestedValueFields) {
        requested_value_fields_len.set(requestedValueFields.length);
        requested_value_fields.set(requestedValueFields);
        java_requested_value_fields = requestedValueFields;
        requested_value_fields_set.set(true);
    }
}
