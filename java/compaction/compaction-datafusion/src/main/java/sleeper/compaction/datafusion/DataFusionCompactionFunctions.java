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
package sleeper.compaction.datafusion;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;

import sleeper.foreign.FFIFileResult;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.bridge.ForeignFunctions;
import sleeper.foreign.datafusion.FFICommonConfig;

/**
 * The interface for the native library we are calling.
 */
public interface DataFusionCompactionFunctions extends ForeignFunctions {

    /**
     * Invokes a native compaction.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context               Java context object
     * @param  input                 compaction input configuration
     * @param  result                compaction result if successful
     * @return                       indication of success
     * @throws IllegalStateException if the context has already been closed
     */
    default int compact(FFIContext<DataFusionCompactionFunctions> context, FFICommonConfig input, FFIFileResult result) {
        return native_compact(context.getForeignContext(), input, result);
    }

    @SuppressWarnings(value = "checkstyle:parametername")
    int native_compact(@In Pointer ctx_ptr, @In FFICommonConfig input_ptr, @Out FFIFileResult output_ptr);

    /**
     * Get number of rows read for a currently executing compaction.
     *
     * The number of rows read by the given job ID is returned in the result parameter. The number of rows written
     * will be 0. If no data can be found for the job, then -1 is returned and the contents of the result object is
     * unspecified.
     *
     * @param  context Java context objet
     * @param  jobId   compaction job ID
     * @param  result  object to populate
     * @return         0 on success, -1 if no row count could be retrieved
     */
    default int get_compaction_rows_read(FFIContext<DataFusionCompactionFunctions> context, String jobId, FFIFileResult result) {
        return native_get_compaction_rows_read(context.getForeignContext(), jobId, result);
    }

    @SuppressWarnings(value = "checkstyle:parametername")
    int native_get_compaction_rows_read(@In Pointer ctx_ptr, String job_id, @Out FFIFileResult output_ptr);
}
