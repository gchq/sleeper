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

import jnr.ffi.LibraryLoader;
import org.scijava.nativelib.JniExtractor;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides low-level bridge functionality for calling foreign code.
 */
class FFIBridge {
    /**
     * Native library extraction object. This can extract native libraries from the classpath and
     * unpack them to a temporary directory.
     */
    private static final JniExtractor EXTRACTOR = NativeLoader.getJniExtractor();
    /**
     * Map for recording which native libraries have been extracted to which path.
     *
     * IMPORTANT: Access to this Map MUST be synchronized.
     */
    private static final Map<String, File> EXTRACTED_NATIVE_LIBS = new HashMap<>();

    /** Paths in the JAR file where a native library may have been placed. */
    private static final String[] LIB_PATHS = {
        "natives/x86_64-unknown-linux-gnu/release", "natives/aarch64-unknown-linux-gnu/release",
        // Rust debug builds will place libraries in different locations
        "natives/x86_64-unknown-linux-gnu/debug", "natives/aarch64-unknown-linux-gnu/debug"};

    private static final Logger LOGGER = LoggerFactory.getLogger(FFIBridge.class);

    /**
     * Attempt to load the native foreign function library.
     *
     * The native library will be extracted from the classpath and unpacked to a temporary
     * directory. The library is then loaded and linked. Multiple locations are checked in the
     * classpath, representing different architectures. Thus, if we attempt to load a library for
     * the wrong CPU architecture, loading will fail and the next path will be tried. This way, we
     * maintain a single JAR file that can work across multiple CPU architectures.
     *
     * Upon successful loading and linking, the path to the extracted library will be cached, therefore
     * subsequent calls to this function for the same library will avoid extracting a fresh copy
     * of the native library.
     *
     * Thread safety: The instance returned by this method is NOT thread safe, therefore if shared across
     * threads, then external synchronization MUST be used by the client.
     *
     * @param  clazz       the interface describing the foreign function calls
     * @param  <T>         interface type containing Java functions stubs
     * @return             the native call interface
     * @throws IOException if an error occurs during loading or linking the native library
     */
    public static synchronized <T extends ForeignFunctions> T createForeignInterface(Class<T> clazz) throws IOException {
        try {
            return cachedLibraryLoad(clazz, "sleeper_df");
        } catch (UnsatisfiedLinkError err) {
            throw (IOException) new IOException("Could not load and link foreign library", err);
        }
    }

    /**
     * Loads the named library after extracting it from the classpath.
     *
     * This function extracts the named library from a JAR on the classpath and attempts to load it
     * and bind it to the given interface class. The paths in the array {@link LIB_PATHS} are tried
     * in order. If a library is found at a path, this method will attempt to load it. If no library
     * is found on the classpath or it can't be loaded (e.g. wrong binary format), the next path
     * will be tried.
     *
     * The library named should be given without platform prefixes, e.g. "foo" will be expanded into
     * "libfoo.so" or "foo.dll" as appropriate for this platform.
     *
     * Upon successful load, the library name and extracted path are cached.
     *
     * This should be called from inside a synchronised context.
     *
     * @param  <T>                  the type of the interface to the native code
     * @param  clazz                the class of the interface to the native code
     * @param  libName              the library name to extract without platform prefixes
     * @return                      the absolute extracted path, or null if the library couldn't be found
     * @throws IOException          if an error occured during file extraction
     * @throws UnsatisfiedLinkError if the library could not be found or loaded
     */
    private static <T> T extractAndLink(Class<T> clazz, String libName) throws IOException {
        // Work through each potential path to see if we can load the library
        // successfully
        for (String path : LIB_PATHS) {
            LOGGER.debug("Attempting to load native library from JAR path {}", path);
            // Attempt extraction
            File extractedLib = EXTRACTOR.extractJni(path, libName);

            // If file located, attempt to load
            if (extractedLib != null) {
                LOGGER.debug("Extracted file is at {}", extractedLib);
                try {
                    T instance = LibraryLoader.create(clazz).failImmediately()
                            .load(extractedLib.getAbsolutePath());
                    // Remember that we can successfully load the library from this path
                    EXTRACTED_NATIVE_LIBS.put(libName, extractedLib);
                    return instance;
                } catch (UnsatisfiedLinkError e) {
                    // wrong library, try the next path
                    LOGGER.warn("Unable to load native library from {}", path, e);
                }
            }
        }

        // No matches
        throw new UnsatisfiedLinkError("Couldn't locate or load " + libName);
    }

    /**
     * Loads a native library from the classpath, using a cached location if possible.
     *
     * If the requested library has already been successfully loaded and linked, then
     * the cached entry will be used. Otherwise, the library will be extracted from the classpath.
     *
     * This should be called from inside a synchronised context.
     *
     * @param  <T>                  the type of the interface to the native code
     * @param  clazz                the class of the interface to the native code
     * @param  libName              the library name to extract without platform prefixes
     * @return                      the absolute extracted path, or null if the library couldn't be found
     * @throws IOException          if an error occured during file extraction
     * @throws UnsatisfiedLinkError if the library could not be found or loaded
     */
    private static <T> T cachedLibraryLoad(Class<T> clazz, String libName) throws IOException {
        File cachedPath = EXTRACTED_NATIVE_LIBS.get(libName);
        if (cachedPath != null) {
            return LibraryLoader.create(clazz).failImmediately().load(cachedPath.getAbsolutePath());
        } else {
            return extractAndLink(clazz, libName);
        }
    }

    private FFIBridge() {
    }
}
