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

import java.util.Objects;
import java.util.Optional;

/**
 * Provides a high level interface to foreign function code.
 *
 * If this class is shared between threads, external synchronisation must be used.
 *
 * Clients should create an instance of this class in a <a
 * href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">
 * try-with-resources</a>
 * construct:
 *
 * <pre>
 * try (FFIContext context = new FFIContext(functions)) {
 *   ...
 * }
 * </pre>
 *
 * @param <T> the interface type of the functions to be called in this context
 */
public class FFIContext<T extends ForeignFunctions> implements AutoCloseable {
    /**
     * FFI call interface. Calling any function on this object will
     * result in an FFI call.
     */
    private final T functions;

    /**
     * Pointer to the Rust side of the FFI layer. If this is empty, it means the
     * context has been closed.
     */
    private Optional<Pointer> context;

    /**
     * Initialises the FFI library and context for calling functions.
     *
     * This will attempt to extract the native library from the JAR file and
     * load it into the JVM. It will then establish the Rust side of the context
     * to enable queries to be executed.
     *
     * @param functions the native function interface
     */
    public FFIContext(T functions) {
        this.functions = Objects.requireNonNull(functions, "functions");
        // Create Java interface to FFI lib
        // Make FFI call to establish foreign context
        this.context = Optional.of(functions.create_context());
    }

    /**
     * Closes this FFI context.
     *
     * Once this function has been called, no further FFI calls can be made using it
     * and will throw exceptions. It is safe to close this context whilst query
     * streams are active; however, no further queries can be executed.
     *
     * This is an idempotent operation, calling it multiple times will have no
     * effect.
     */
    @Override
    public void close() {
        // if we have a pointer, then make FFI call to destroy resources
        context = context.map(val -> {
            functions.destroy_context(val);
            // set pointer to null to prevent double closing
            return null;
        });
    }

    /**
     * Checks if this context has been closed.
     *
     * @return true if context is closed
     */
    public boolean isClosed() {
        return context.isEmpty();
    }

    /**
     * Throws an exception is this context is closed.
     *
     * @throws IllegalStateException if this context has been closed
     */
    private void checkClosed() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("FFIContext already closed");
        }
    }

    public T getFunctions() {
        return functions;
    }

    /**
     * Gets a pointer to the foreign context object.
     *
     * @return                       foreign pointer
     * @throws IllegalStateException if this context has already been closed
     */
    public Pointer getForeignContext() {
        checkClosed();
        return context.get();
    }
}
