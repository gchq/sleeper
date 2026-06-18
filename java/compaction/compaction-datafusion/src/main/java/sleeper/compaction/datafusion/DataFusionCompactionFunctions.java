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
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.types.size_t;

import sleeper.foreign.FFIFileResult;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.bridge.ForeignFunctions;
import sleeper.foreign.datafusion.FFICommonConfig;

/**
 * The interface for the native library we are calling.
 */
public interface DataFusionCompactionFunctions extends ForeignFunctions {

    /**
     * Type for callback functions for compactions.
     *
     * <strong>Technical notes:</strong>
     * <ul>
     * <li>do <strong>NOT</strong> make any assumptions about which thread calls this function! It will not be the
     * thread
     * that invoked the native compaction, and it may not always be the same native thread that calls this function
     * during
     * a single compaction! Therefore, any mutable state changes made by implementations of this interface, must be
     * appropriately synchronised.</li>
     * <li>This function should execute quickly. If you need to do further work, consider using a thread-safe container
     * to
     * hand work off to a separate Java thread.</li>
     * <li>Do not make further native calls inside a callback function</li>
     * </ul>
     */
    @FunctionalInterface
    interface ProgressCallback {
        /**
         * Progress callback function.
         *
         * @param rows number of rows written during a compaction
         */
        @Delegate
        void rowsWritten(@size_t long rows);
    }

    /**
     * Invokes a native compaction.
     *
     * Progress updates are delivered periodically by a native thread calling the given progress function. This
     * parameter may be null.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context               Java context object
     * @param  input                 compaction input configuration
     * @param  result                compaction result if successful
     * @param  progressCallback      optional function to be called with progress updates
     * @return                       indication of success
     * @throws IllegalStateException if the context has already been closed
     */
    default int compact(FFIContext<DataFusionCompactionFunctions> context, FFICommonConfig input, FFIFileResult result, ProgressCallback progressCallback) {
        return native_compact(context.getForeignContext(), input, result, progressCallback);
    }

    @SuppressWarnings(value = "checkstyle:parametername")
    int native_compact(@In Pointer ctx_ptr, @In FFICommonConfig input_ptr, @Out FFIFileResult output_ptr, ProgressCallback progressCallback);
}
