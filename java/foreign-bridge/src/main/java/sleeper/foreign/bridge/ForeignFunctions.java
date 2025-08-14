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
package sleeper.foreign.bridge;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.In;

/**
 * Base interface for Sleeper foreign code functionality.
 */
public interface ForeignFunctions {
    /**
     * FFI call to create the necessary state in the native library.
     * <p>
     * Calling this method will create the necessary state on the foreign side
     * of the FFI boundary and perform whatever initialisation is required.
     * <p>
     * The returned pointer should be treated as an opaque handle to the created
     * data. No further meaning to its intent should be conferred on it, that is,
     * the value will change between executions and the specific address pointed to
     * by the pointer has no specific meaning.
     * <p>
     * <strong>DO NOT</strong> attempt to
     * read or write any data behind the pointer as this will lead to undefined
     * behaviour and most likely unpredictable crashes.
     * <p>
     * <strong>Note:</strong> It is the callers responsibility to call
     * {@link ForeignFunctions#destroy_context(Pointer)} when this context is no
     * longer required, otherwise the resources attached to this context will leak.
     *
     * @see    FFIContext
     *
     * @return a handle to the created context
     */
    Pointer create_context();

    /**
     * FFI call to destroy a previously allocated context.
     * <p>
     * This function will safely destroy and de-allocate all resources and memory
     * associated with the given context.
     * <p>
     * Only handles previously created by {@link ForeignFunctions#create_context()}
     * should be passed to this function.
     * <p>
     * <strong>It is undefined behaviour to pass a null or invalid pointer to this
     * function.</strong>
     *
     * @param ctx the handle to the context to destroy
     */
    void destroy_context(@In Pointer ctx);

}
