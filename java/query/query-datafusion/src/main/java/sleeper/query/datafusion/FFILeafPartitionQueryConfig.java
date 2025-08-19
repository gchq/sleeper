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
package sleeper.query.datafusion;

import jnr.ffi.Struct;

import sleeper.foreign.FFISleeperRegion;
import sleeper.foreign.datafusion.FFICommonConfig;

/**
 * The leaf query data that will be populated from the Java side.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
public class FFILeafPartitionQueryConfig extends Struct {
    /** Basic configuration for query. */
    public final Struct.StructRef<FFICommonConfig> common = new StructRef<>(FFICommonConfig.class);
    public final Struct.size_t query_region_len = new Struct.size_t();
    /** The list of query regions. */
    public final Struct.StructRef<FFISleeperRegion> query_regions = new StructRef<>(FFISleeperRegion.class);
    /** Specifies quantile sketches be written out to a file. */
    public final Struct.Boolean write_quantile_sketch = new Struct.Boolean();
    /** Specifies if logical and physical DataFusion query plans should be written to a log output. */
    public final Struct.Boolean explain_plans = new Struct.Boolean();

    public FFILeafPartitionQueryConfig(
            jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Sets query region field and length.
     *
     * @param regions query regions
     */
    public void setQueryRegions(FFISleeperRegion[] regions) {
        for (int i = 0; i < regions.length; i++) {
            regions[i].validate();
        }
        query_region_len.set(regions.length);
        query_regions.set(regions);
    }
}
