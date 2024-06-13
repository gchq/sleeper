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
package sleeper.compaction.job.execution;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.LibraryLoader;
import jnr.ffi.NativeType;
import jnr.ffi.Struct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import org.scijava.nativelib.JniExtractor;
import org.scijava.nativelib.NativeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

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
        "natives/x86_64-unknown-linux-gnu/debug", "natives/aarch64-unknown-linux-gnu/debug"};

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
     * @return             the native compaction object
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
     * @param  clazz                the Java interface type for the native library
     * @param  libName              the library name to extract without platform prefixes.
     * @return                      the absolute extracted path, or null if the library couldn't be found
     * @throws IOException          if an error occured during file extraction
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
     * The compaction input data that will be populated from the Java side. If you updated
     * this struct (field ordering, types, etc.), you MUST update the corresponding Rust definition
     * in rust/compaction/src/lib.rs.
     */
    @SuppressWarnings(value = {"checkstyle:membername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class FFICompactionParams extends Struct {
        /** Array of input files to compact. */
        public final Array<java.lang.String> input_files = new Array<>(this);
        /** Output file name. */
        public final Struct.UTF8StringRef output_file = new Struct.UTF8StringRef();
        /** Names of Sleeper row key columns from schema. */
        public final Array<java.lang.String> row_key_cols = new Array<>(this);
        /** Types for region schema 1 = Int, 2 = Long, 3 = String, 4 = Byte array. */
        public final Array<java.lang.Integer> row_key_schema = new Array<>(this);
        /** Names of Sleeper sort key columns from schema. */
        public final Array<java.lang.String> sort_key_cols = new Array<>(this);
        /** Maximum size of output Parquet row group in rows. */
        public final Struct.size_t max_row_group_size = new Struct.size_t();
        /** Maximum size of output Parquet page size in bytes. */
        public final Struct.size_t max_page_size = new Struct.size_t();
        /** Output Parquet compression codec. */
        public final Struct.UTF8StringRef compression = new Struct.UTF8StringRef();
        /** Output Parquet writer version. Must be 1.0 or 2.0 */
        public final Struct.UTF8StringRef writer_version = new Struct.UTF8StringRef();
        /** Column min/max values truncation length in output Parquet. */
        public final Struct.size_t column_truncate_length = new Struct.size_t();
        /** Max sizeof statistics block in output Parquet. */
        public final Struct.size_t stats_truncate_length = new Struct.size_t();
        /** Should row key columns use dictionary encoding in output Parquet. */
        public final Struct.Boolean dict_enc_row_keys = new Struct.Boolean();
        /** Should sort key columns use dictionary encoding in output Parquet. */
        public final Struct.Boolean dict_enc_sort_keys = new Struct.Boolean();
        /** Should value columns use dictionary encoding in output Parquet. */
        public final Struct.Boolean dict_enc_values = new Struct.Boolean();
        /** Compaction partition region minimums. MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<Object> region_mins = new Array<>(this);
        /** Compaction partition region maximums. MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<Object> region_maxs = new Array<>(this);
        /** Compaction partition region minimums are inclusive? MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<java.lang.Boolean> region_mins_inclusive = new Array<>(this);
        /** Compaction partition region maximums are inclusive? MUST BE SAME LENGTH AS row_key_cols. */
        public final Array<java.lang.Boolean> region_maxs_inclusive = new Array<>(this);

        public FFICompactionParams(jnr.ffi.Runtime runtime) {
            super(runtime);
        }

        /**
         * Validate state of struct.
         *
         * @throws IllegalStateException when a invariant fails
         */
        public void validate() {
            input_files.validate();
            row_key_cols.validate();
            row_key_schema.validate();
            sort_key_cols.validate();
            region_mins.validate();
            region_maxs.validate();
            region_mins_inclusive.validate();
            region_maxs_inclusive.validate();

            // Check strings non null
            Objects.requireNonNull(output_file.get(), "Output file is null");
            Objects.requireNonNull(writer_version, "Parquet writer is null");
            Objects.requireNonNull(compression, "Parquet compression codec is null");

            // Check lengths
            long rowKeys = row_key_cols.len.get();
            if (rowKeys != row_key_schema.len.get()) {
                throw new IllegalStateException("row key schema array has length " + row_key_schema.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_maxs.len.get()) {
                throw new IllegalStateException("region maxs has length " + region_maxs.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_mins.len.get()) {
                throw new IllegalStateException("region mins has length " + region_mins.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_mins_inclusive.len.get()) {
                throw new IllegalStateException("region mins inclusives has length " + region_mins_inclusive.len.get() + " but there are " + rowKeys + " row key columns");
            }
            if (rowKeys != region_maxs_inclusive.len.get()) {
                throw new IllegalStateException("region maxs inclusives has length " + region_maxs_inclusive.len.get() + " but there are " + rowKeys + " row key columns");
            }
        }
    }

    /**
     * Array class that can be inside a Struct. Creates a dynamic array that can be passed to C.
     * Strong references are maintained for allocated memory so GC will dispose of memory when
     * this object is collected.
     *
     * @param <T> object type of array
     */
    public static class Array<T> {
        // Length of array
        public final Struct.size_t len;
        // Pointer to base of dynamically allocated array
        public final Struct.Pointer arrayBase;
        // Reference to dynamically allocated array to prevent GC until Array instance is collected
        public jnr.ffi.Pointer basePtr;
        // Reference to dynamically allocated items to prevent GC until Array instance is collected
        public jnr.ffi.Pointer[] items;

        public Array(Struct enclosing) {
            this.len = enclosing.new size_t();
            this.arrayBase = enclosing.new Pointer();
        }

        /**
         * Create a dynamic array of items in this array.
         *
         * A base pointer is allocated pointers set to other
         * dynamically allocated memory containing items from array.
         *
         * @param  arr                  array data
         * @param  nullsAllowed         if null pointers are allowed in the data array
         * @throws NullPointerException if a null is found but not allowed
         */
        public void populate(final T[] arr, boolean nullsAllowed) {
            final jnr.ffi.Runtime r = len.struct().getRuntime();
            // Calculate size needed for array of pointers
            int ptrSize = r.findType(NativeType.ADDRESS).size();
            // Null out zero length arrays
            if (arr.length > 0) {
                int size = arr.length * ptrSize;
                // Allocate some memory for pointers
                this.basePtr = r.getMemoryManager().allocateDirect(size);
                this.arrayBase.set(basePtr);

                this.items = new jnr.ffi.Pointer[arr.length];

                for (int i = 0; i < arr.length; i++) {
                    if (!nullsAllowed && arr[i] == null) {
                        throw new NullPointerException("Index " + i + " of array is null when nulls aren't allowed here");
                    }
                    setValue(arr[i], i, r);
                }

                // Bulk set the pointers in the base array
                this.basePtr.put(0, this.items, 0, this.items.length);
            } else {
                this.basePtr = null;
                this.items = null;
            }

            // Set length of array in struct
            this.len.set(arr.length);
        }

        /**
         * Check class invariants.
         *
         * @throws IllegalStateException if a violation is found
         */
        public void validate() {
            if (len.get() == 0) {
                if (basePtr != null || items != null) {
                    throw new IllegalStateException("array length is 0 but pointers not null");
                }
            } else {
                if (len.get() != items.length) {
                    throw new IllegalStateException("length of " + len.get() + " doesn't match items length of " + items.length);
                }
                Objects.requireNonNull(this.basePtr, "base pointer is null");
                Objects.requireNonNull(this.items, "items array is null");
                if (this.arrayBase.get().address() != this.basePtr.address()) {
                    throw new IllegalStateException("array base pointer and stored base pointer differ!");
                }
            }
        }

        /**
         * Sets a given value in the array to a specific value. The data
         * is byte encoded.
         *
         * Intended for internal use only.
         *
         * @param  <E>                       item type, must be int, long String or byte[], or boolean
         * @param  item                      the item to encode
         * @param  idx                       array position to use
         * @param  r                         struct runtime
         * @throws ClassCastException        if item is of wrong class
         * @throws IndexOutOfBoundsException if idx is invalid
         */
        protected <E> void setValue(E item, int idx, jnr.ffi.Runtime r) {
            if (item == null) {
                this.items[idx] = jnr.ffi.Pointer.wrap(r, 0);
            } else if (item instanceof Integer) {
                int e = (int) item;
                this.items[idx] = r.getMemoryManager().allocateDirect(r.findType(NativeType.SINT).size());
                this.items[idx].putInt(0, e);
            } else if (item instanceof Long) {
                long e = (long) item;
                this.items[idx] = r.getMemoryManager().allocateDirect(r.findType(NativeType.SLONGLONG).size());
                this.items[idx].putLong(0, e);
            } else if (item instanceof java.lang.String) {
                // Strings are encoded as 4 byte length then value
                java.lang.String e = (java.lang.String) item;
                byte[] utf8string = e.getBytes(StandardCharsets.UTF_8);
                // Add four for length
                int stringSize = utf8string.length + 4;
                // Allocate memory for string and write length then the string
                this.items[idx] = r.getMemoryManager().allocateDirect(stringSize);
                this.items[idx].putInt(0, utf8string.length);
                this.items[idx].put(4, utf8string, 0, utf8string.length);
            } else if (item instanceof byte[]) {
                // Byte arrays are encoded as 4 byte length then value
                byte[] e = (byte[]) item;
                int byteSize = e.length + 4;
                this.items[idx] = r.getMemoryManager().allocateDirect(byteSize);
                this.items[idx].putInt(0, e.length);
                this.items[idx].put(4, e, 0, e.length);
            } else if (item instanceof Boolean) {
                boolean e = (boolean) item;
                this.items[idx] = r.getMemoryManager().allocateDirect(1);
                this.items[idx].putByte(0, e ? (byte) 1 : (byte) 0);
            } else {
                throw new ClassCastException("Can't cast " + item.getClass() + " to a valid Sleeper row key type");
            }
        }
    }

    /**
     * The compaction output data that the native code will populate.
     */
    @SuppressWarnings(value = {"checkstyle:membername", "checkstyle:parametername"})
    @SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
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
        int ffi_merge_sorted_files(@In FFICompactionParams input, @Out FFICompactionResult result);
    }

    private RustBridge() {
    }
}
