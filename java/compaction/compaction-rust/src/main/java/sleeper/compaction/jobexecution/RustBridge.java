/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.compaction.jobexecution;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Struct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.types.size_t;
import org.scijava.nativelib.JniExtractor;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class RustBridge {
    /**
     * Native library extraction object. This can extract native libraries from the classpath and
     * unpack them to a temporary directory.
     */
    private static final JniExtractor EXTRACTOR = NativeLoader.getJniExtractor();

    /** Paths in the JAR file where a native library may have been placed. */
    private static final String[] LIB_PATHS = {"natives/release",
            "natives/x86_64-unknown-linux-gnu/release", "natives/aarch64-unknown-linux-gnu/release",
            // Rust debug builds will place libraries in different locations
            "natives/x86_64-unknown-linux-gnu/debug", "natives/aarch64-unknown-linux-gnu/debug" };

    private static final Logger LOGGER = LoggerFactory.getLogger(RustBridge.class);

    private static Compaction nativeCompaction = null;

    /**
     * Attempt to load the native compaction library.
     *
     * The native library will be extracted from the classpath and unpacked to a temporary
     * directory. The library is then loaded and linked. Multiple locations are checked in the
     * classpath, representing different architectures. Thus, if we attempt to load a library for
     * the wrong CPU architecture, loading will fail and the next path will be tried. This way, we
     * maintain a single JAR file that can work across multiple CPU architectures.
     *
     * @return the native compaction object
     * @throws IOException if an error occurs during loading or linking the native library
     */
    public static synchronized Compaction getRustCompactor() throws IOException {
        try {
            Compaction nativeLib;

            if (nativeCompaction == null) {
                nativeLib = extractAndLink(Compaction.class, "compaction");
                nativeCompaction = nativeLib;
            } else {
                nativeLib = nativeCompaction;
            }

            return nativeLib;

        } catch (UnsatisfiedLinkError err) {
            throw (IOException) new IOException().initCause(err);
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
     * @param clazz the Java interface type for the native library
     * @param libName the library name to extract without platform prefixes.
     * @return the absolute extracted path, or null if the library couldn't be found
     * @throws IOException if an error occured during file extraction
     * @throws UnsatisfiedLinkError if the library could not be found or loaded
     */
    public static <T> T extractAndLink(Class<T> clazz, String libName) throws IOException {
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
                    return LibraryLoader.create(clazz).failImmediately()
                            .load(extractedLib.getAbsolutePath());
                } catch (UnsatisfiedLinkError e) {
                    // wrong library, try the next path
                    LOGGER.error("Unable to load native library from " + path, e);
                }
            }
        }

        // No matches
        throw new UnsatisfiedLinkError("Couldn't locate or load " + libName);
    }

    /**
     * The compaction output data that the native code will populate.
     */
    @SuppressWarnings(value = { "checkstyle:membername", "checkstyle:parametername" })
    public static class FFICompactionResult extends Struct {
        public final Struct.size_t rows_read = new Struct.size_t();
        public final Struct.size_t rows_written = new Struct.size_t();

        public FFICompactionResult(jnr.ffi.Runtime runtime) {
            super(runtime);
        }
    }

    /**
     * The interface for the native library we are calling.
     */
    public interface Compaction {
        FFICompactionResult allocate_result();

        void free_result(@In FFICompactionResult res);

        @SuppressWarnings(value = "checkstyle:parametername")
        int ffi_merge_sorted_files(@In String[] input_file_paths, @size_t long input_file_paths_len,
                String output_file_path, @size_t long row_group_size, @size_t long max_page_size,
                @In long[] row_key_columns, @size_t long row_key_columns_len,
                @In long[] sort_columns, @size_t long sort_columns_len,
                @Out FFICompactionResult result);
    }

    private RustBridge() {
    }
}
