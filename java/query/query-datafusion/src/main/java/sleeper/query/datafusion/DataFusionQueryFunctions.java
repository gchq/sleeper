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
import jnr.ffi.annotations.Synchronized;

import sleeper.foreign.FFIFileResult;
import sleeper.foreign.bridge.FFIContext;
import sleeper.foreign.bridge.ForeignFunctions;

import java.util.Optional;

/**
 * Calls the native library with query functionality.
 */
@Synchronized
public interface DataFusionQueryFunctions extends ForeignFunctions {

    /**
     * Retrives the link to the DataFusion code in Rust.
     *
     * @return                       the Rust DataFusion implementation
     * @throws IllegalStateException if the DataFusion implementation failed to link
     */
    static DataFusionQueryFunctions getInstance() {
        return DataFusionQueryFunctionsIfLoaded.INSTANCE.getFunctionsOrThrow();
    }

    /**
     * Retrives the link to the DataFusion code in Rust, unless the link failed to load.
     *
     * @return the Rust DataFusion implementation, if it loaded
     */
    static Optional<DataFusionQueryFunctions> getInstanceIfLoaded() {
        return DataFusionQueryFunctionsIfLoaded.INSTANCE.getFunctionsIfLoaded();
    }

    static Exception getLoadingFailure() {
        return DataFusionQueryFunctionsIfLoaded.INSTANCE.getLoadingFailure();
    }

    /**
     * Invokes a native query. Returns a stream of Arrow record batches.
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

    public static class Foo {
    }

    static final Foo SYNC = new Foo();

    default int query_stream(FFIContext<DataFusionQueryFunctions> context, FFILeafPartitionQueryConfig input, FFIQueryResults outputStream) {
        synchronized (SYNC) {
            System.err.println("\n\nquery stream Holds lock on " + System.identityHashCode(SYNC) + " " + Thread.holdsLock(SYNC));
            return native_query_stream(context.getForeignContext(), input, outputStream);
        }
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
    @Synchronized
    int native_query_stream(@In Pointer context, @In FFILeafPartitionQueryConfig input, @Out FFIQueryResults outputStream);

    /**
     * Invokes a native query. This function returns the results of writing data to a file.
     *
     * The provided context object must be open.
     * The return code will be 0 if successful.
     *
     * @param  context               Java context object
     * @param  input                 query input configuration
     * @param  outputResult          stream of results if successful
     * @return                       indication of success
     * @throws IllegalStateException if the context has already been closed
     */
    default int query_file(FFIContext<DataFusionQueryFunctions> context, FFILeafPartitionQueryConfig input, FFIFileResult outputResult) {
        return native_query_file(context.getForeignContext(), input, outputResult);
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
     * @param  outputResult stream of results if successful
     * @return              indication of success
     */
    @SuppressWarnings(value = "checkstyle:parametername")
    int native_query_file(@In Pointer context, @In FFILeafPartitionQueryConfig input, @Out FFIFileResult outputResult);
}
