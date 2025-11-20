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

import java.io.IOException;
import java.util.Objects;

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
     * The class type for the FFI interface.
     */
    private final Class<T> ffiClass;
    /**
     * FFI call interface. Calling any function on this object will
     * result in an FFI call.
     */
    private final T functions;

    /**
     * Pointer to the Rust side of the FFI layer. If this is null, it means the
     * context has been closed.
     */
    private volatile Pointer context;

    /**
     * Initialises the FFI library and context for calling functions.
     *
     * This will attempt to extract the native library from the JAR file and
     * load it into the JVM. It will then establish the Rust side of the context
     * to enable FFI calls to be executed.
     *
     * @param  functionClass        the native function interface type
     * @throws NullPointerException if any parameter is null
     * @throws IOException          if the native library couldn't be loaded
     */
    public FFIContext(Class<T> functionClass) throws IOException {
        this.ffiClass = Objects.requireNonNull(functionClass, "functionClass must not be null");
        this.functions = FFIBridge.createForeignInterface(functionClass);
        this.context = Objects.requireNonNull(functions.create_context(), "FFI create_context returned null");
    }

    /**
     * Clones this context from an existing one.
     *
     * This is useful for creating a new context instance from an existing one to use on a new thread.
     *
     * @param  original              the context to clone
     * @throws IOException           if the native library couldn't be loaded
     * @throws IllegalStateException if the original context has already been closed
     */
    public FFIContext(FFIContext<T> original) throws IOException {
        original.checkClosed();
        this.ffiClass = original.ffiClass;
        this.functions = FFIBridge.createForeignInterface(ffiClass);
        this.context = Objects.requireNonNull(functions.clone_context(original.context), "FFI clone_context returned null");
    }

    /* Internal test constructor. */
    FFIContext(Class<T> functionClass, T functions) {
        this.ffiClass = Objects.requireNonNull(functionClass, "functionClass");
        this.functions = Objects.requireNonNull(functions, "functions");
        this.context = functions.create_context();
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
        if (context != null) {
            functions.destroy_context(context);
            context = null;
        }
    }

    /**
     * Checks if this context has been closed.
     *
     * @return true if context is closed
     */
    public boolean isClosed() {
        return context == null;
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
        return context;
    }
}
