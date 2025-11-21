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
     * FFI call interface. Calling any function on this object will
     * result in an FFI call.
     */
    private final T functions;
    /**
     * Pointer to the Rust side of the FFI layer. If this is null, it means the
     * context has been closed.
     */
    private Pointer context;
    /** Class lock for synchronization. */
    private static final Object CONTEXT_LOCK = new Object();
    /** Last created context. */
    private static FFIContext<?> lastContext = null;

    /**
     * Initialises the FFI library and context for calling functions.
     *
     * This will attempt to extract the native library from the JAR file and
     * load it into the JVM. It will then establish the Rust side of the context
     * to enable FFI calls to be executed. If possible a new context will be created from an
     * existing one.
     *
     * This method is thread-safe.
     *
     * @param  functions   the FFI functions instance
     * @param  context     the foreign context pointer
     * @throws IOException if the native library couldn't be loaded
     */
    FFIContext(T functions, Pointer context) throws IOException {
        this.functions = functions;
        this.context = context;
    }

    /**
     * Creates an FFI context.
     *
     * This will either create a new FFI context object on the foreign side, or clone from an existing one. An existing
     * foreign context will only be used if it is still open.
     *
     * This method is thread safe.
     *
     * @param  <T>           the interface type of the functions to be called in this context
     * @param  functionClass class type for the FFI interface
     * @return               a valid and open FFIContext for making FFI calls
     * @throws IOException   if the native library couldn't be loaded
     */
    public static <T extends ForeignFunctions> FFIContext<T> getFFIContext(Class<T> functionClass) throws IOException {
        synchronized (CONTEXT_LOCK) {
            T functions = FFIBridge.createForeignInterface(Objects.requireNonNull(functionClass, "functionClass"));
            FFIContext<T> newContext;
            if (lastContext != null && !lastContext.isClosed()) {
                newContext = new FFIContext<>(functions, functions.clone_context(lastContext.context));
                lastContext = newContext;
            } else {
                newContext = new FFIContext<>(functions, functions.create_context());
                lastContext = newContext;
            }
            return newContext;
        }
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
        synchronized (CONTEXT_LOCK) {
            // if we have a pointer, then make FFI call to destroy resources
            if (context != null) {
                functions.destroy_context(context);
                context = null;
            }
        }
    }

    /**
     * Checks if this context has been closed.
     *
     * @return true if context is closed
     */
    public boolean isClosed() {
        synchronized (CONTEXT_LOCK) {
            return context == null;
        }
    }

    /**
     * Throws an exception is this context is closed.
     *
     * @throws IllegalStateException if this context has been closed
     */
    private void checkOpen() throws IllegalStateException {
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
        checkOpen();
        return context;
    }
}
