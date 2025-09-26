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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Pointer;
import jnr.ffi.Struct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;

import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.bridge.ForeignFunctions;

/**
 * The interface for the native library we are calling.
 */
public interface DataFusionCompactionFunctions extends ForeignFunctions {

    DataFusionCompactionFunctions INSTANCE = DataFusionCompactionFunctionsImpl.create();

    /**
     * The compaction output data that the native code will populate.
     */
    @SuppressWarnings(value = {"checkstyle:membername", "checkstyle:parametername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    class DataFusionCompactionResult extends Struct {
        public final Struct.size_t rows_read = new Struct.size_t();
        public final Struct.size_t rows_written = new Struct.size_t();

        public DataFusionCompactionResult(jnr.ffi.Runtime runtime) {
            super(runtime);
        }
    }

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
    default int compact(FFIContext context, DataFusionCommonConfig input, DataFusionCompactionResult result) {
        return native_compact(context.getForeignContext(), input, result);
    }

    @SuppressWarnings(value = "checkstyle:parametername")
    int native_compact(Pointer context, @In DataFusionCommonConfig input, @Out DataFusionCompactionResult result);
}
