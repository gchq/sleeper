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
import jnr.ffi.provider.jffi.ArrayMemoryIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.util.Objects;

/**
 * Provides a high level interface to foreign function code.
 *
 * If this class is shared between threads, external synchronisation must be
 * used.
 *
 * Clients should create an instance of this class in a <a
 * href=
 * "https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">
 * try-with-resources</a> construct:
 *
 * <pre>
 * Class<SomeFunctionClass> functions = ...;
 * try (FFIContext<SomeFunctionClass> context = getFFIContextSafely(functions)) {
 *   ...
 * }
 * </pre>
 *
 * Thread safety: This class is not thread safe! Do NOT attempt to re-use
 * instances of this class across multiple threads without external synchronisation.
 * A better strategy is to create new context objects for each thread.
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

    private static final Object CONTEXT_LOCK = new Object();
    private static FFIContext<?> rootContext = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(FFIContext.class);

    /**
     * Initialises the FFI library and context for calling functions.
     *
     * This will attempt to extract the native library from the JAR file and
     * load it into the JVM. It will then establish the Rust side of the context
     * to enable FFI calls to be executed. If possible a new context will be created
     * from an existing one.
     *
     * This method is thread-safe.
     *
     * @param functions the FFI functions instance
     * @param context   the foreign context pointer
     */
    FFIContext(T functions, Pointer context) {
        this.functions = functions;
        this.context = context;
    }

    /**
     * Creates an FFI context.
     *
     * This will either create a new FFI context object on the foreign side, or
     * clone from an existing "root" one. The root context is generally never closed.
     *
     * This method is thread safe.
     *
     * @param <T>           the interface type of the functions to be called in this
     *                      context
     * @param functionClass class type for the FFI interface
     * @return a valid, open FFIContext for making FFI calls
     * @throws UncheckedIOException if the native library couldn't be loaded
     * @see FFIContext#closeRootContext()
     */
    public static <T extends ForeignFunctions> FFIContext<T> getFFIContext(Class<T> functionClass)
            throws UncheckedIOException {
        synchronized (CONTEXT_LOCK) {
            try {
                T functions = FFIBridge.createForeignInterface(Objects.requireNonNull(functionClass, "functionClass"));
                if (rootContext == null) {
                    rootContext = new FFIContext<>(functions, functions.create_context());
                }
                rootContext.checkOpen();
                return new FFIContext<>(functions, functions.clone_context(rootContext.context));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Creates an FFI context with fallback if native library can't be loaded.
     *
     * This behaves exactly as {@link FFIContext#getForeignContext()}, but instead
     * of throwing an exception on failure, a dummy context is returned. The dummy context
     * may be open and closed, but will throw {@link UnsupportedOperationException} if any foreign
     * functions are called on it.
     *
     * @param <T>           the interface type of the functions to be called in this
     *                      context
     * @param functionClass class type for the FFI interface
     * @return a valid, open FFIContext for making FFI calls OR a dummy context as a fallback
     */
    public static <T extends ForeignFunctions> FFIContext<T> getFFIContextSafely(Class<T> functionClass) {
        try {
            return getFFIContext(functionClass);
        } catch (UncheckedIOException e) {
            LOGGER.warn("Couldn't load native Sleeper library", e);
            return createDummyContext(functionClass, e);
        }
    }

    static <T extends ForeignFunctions> FFIContext<T> createDummyContext(Class<T> functionClass, Exception e) {
        // create a dynamic proxy that implements T
        @SuppressWarnings("unchecked")
        T functions = (T) Proxy.newProxyInstance(functionClass.getClassLoader(), new Class<?>[] {functionClass},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "create_context":
                            return new ArrayMemoryIO(jnr.ffi.Runtime.getSystemRuntime(), 1);
                        case "destroy_context":
                            return null;
                        // All other methods from type T will throw when called
                        case "clone_context":
                        default:
                            throw new UnsupportedOperationException(
                                    "The native sleeper_df library is not loaded, native implementation not available",
                                    e);
                    }
                });
        return new FFIContext<T>(functions, functions.create_context());
    }

    /**
     * Closes the root FFI context.
     *
     * Other FFI contexts created from this root context will remain open and valid.
     * Once all contexts are closed, any foreign resources will be automatically released.
     */
    public static void closeRootContext() {
        synchronized (CONTEXT_LOCK) {
            if (rootContext != null && !rootContext.isClosed()) {
                rootContext.close();
                rootContext = null;
            }
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
     * @return foreign pointer
     * @throws IllegalStateException if this context has already been closed
     */
    public Pointer getForeignContext() {
        checkOpen();
        return context;
    }
}
