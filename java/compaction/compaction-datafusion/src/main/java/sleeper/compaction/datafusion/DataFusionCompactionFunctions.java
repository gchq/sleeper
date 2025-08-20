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
package sleeper.compaction.datafusion;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;

import sleeper.foreign.FFIFileResult;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.bridge.ForeignFunctions;
import sleeper.foreign.datafusion.FFICommonConfig;

/**
 * The interface for the native library we are calling with compaction functionality.
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
    default int compact(FFIContext context, FFICommonConfig input, FFIFileResult result) {
        return native_compact(context.getForeignContext(), input, result);
    }

    @SuppressWarnings(value = "checkstyle:parametername")
    int native_compact(@In Pointer context, @In FFICommonConfig input, @Out FFIFileResult result);
}
