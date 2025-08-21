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

import jnr.ffi.Pointer;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;

import sleeper.foreign.FFIFileResult;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.bridge.ForeignFunctions;

/**
 * The interface for the native library we are calling with query functionality.
 */
public interface DataFusionQueryFunctions extends ForeignFunctions {

    /**
     * Invokes a native query. This function returns a stream of Arrow record batches.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context               Java context object
     * @param  input                 query input configuration
     * @param  outputStream          stream of results if successful
     * @return                       indication of success
     * @throws IllegalStateException if the context has already been closed
     */
    default int query_stream(FFIContext context, FFILeafPartitionQueryConfig input, FFIQueryResults outputStream) {
        return native_query_stream(context.getForeignContext(), input, outputStream);
    }

    /**
     * FFI method to call to foreign query code. This function returns a stream of Arrow record batches.
     *
     * Clients should generally call
     * {@link DataFusionQueryFunctions#query_stream(FFIContext, FFILeafPartitionQueryConfig, FFIQueryResults)}.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context      pointer to opaque context object
     * @param  input        query input configuration
     * @param  outputStream stream of results if successful
     * @return              indication of success
     */
    @SuppressWarnings(value = "checkstyle:parametername")
    int native_query_stream(@In Pointer context, @In FFILeafPartitionQueryConfig input, @Out FFIQueryResults outputStream);

    /**
     * Invokes a native query. This function returns the results of writing data to a file.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context               Java context object
     * @param  input                 query input configuration
     * @param  outputStream          stream of results if successful
     * @return                       indication of success
     * @throws IllegalStateException if the context has already been closed
     */
    default int query_file(FFIContext context, FFILeafPartitionQueryConfig input, FFIQueryResults outputStream) {
        return native_query_stream(context.getForeignContext(), input, outputStream);
    }

    /**
     * FFI method to call to foreign query code. This function returns the results of writing data to a file.
     *
     * Clients should generally call
     * {@link DataFusionQueryFunctions#query_file(FFIContext, FFILeafPartitionQueryConfig, FFIQueryResults)}.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context      pointer to opaque context object
     * @param  input        query input configuration
     * @param  outputStream stream of results if successful
     * @return              indication of success
     */
    @SuppressWarnings(value = "checkstyle:parametername")
    int native_query_file(@In Pointer context, @In FFILeafPartitionQueryConfig input, @Out FFIFileResult outputResults);
}
