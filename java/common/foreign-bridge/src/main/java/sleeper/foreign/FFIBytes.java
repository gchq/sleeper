/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.foreign;

import jnr.ffi.Struct;
import jnr.ffi.TypeAlias;

/**
 * A Java implementation of FFIBytes. Whilst this class is not a JNR-FFI {@link Struct} instance, it is used
 * to write compatible data at a given memory location. Memory is dynamically allocated for the internal buffer which
 * will be automatically freed when its owning instance is garbage collected.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
public class FFIBytes {
    /** Length of stored data. */
    private final int length;
    /**
     * Memory address of byte array in memory.
     */
    private final jnr.ffi.Pointer data;

    public FFIBytes(jnr.ffi.Runtime runtime, byte[] data) {
        // Allocate some memory for the data
        this.data = runtime.getMemoryManager().allocateDirect(data.length);
        this.data.put(0, data, 0, data.length);
        this.length = data.length;
    }

    private FFIBytes(int length, jnr.ffi.Pointer data) {
        this.length = length;
        this.data = data;
    }

    /**
     * Writes this struct to the given memory address.
     *
     * The memory address must point to a valid location.
     *
     * @param location memory address to write to
     */
    public void writeTo(jnr.ffi.Pointer location) {
        location.putLongLong(0, length);
        location.putPointer(data.getRuntime().findType(TypeAlias.size_t).size(), data);
    }

    /**
     * Read an instance from the given location.
     *
     * The internal data buffer pointer is taken from the given location. It is the callers
     * responsibility to ensure it remains valid for the life of this object.
     *
     * @param  location
     * @return          an FFIBytes deserialised from the given memory location.
     */
    public static FFIBytes readFrom(jnr.ffi.Pointer location) {
        int length = (int) location.getLongLong(0);
        jnr.ffi.Pointer data = location.getPointer(location.getRuntime().findType(TypeAlias.size_t).size());
        return new FFIBytes(length, data);
    }

    /**
     * Retrieve a Java copy of the internal data buffer. This is a snapshot of the data.
     *
     * @return copy of internal byte array
     */
    public byte[] getData() {
        byte[] result = new byte[length];
        data.get(0, result, 0, length);
        return result;
    }

    /**
     * Get the native size of this struct.
     *
     * @param  r the JNR FFI runtime to use
     * @return   struct size in bytes
     */
    public static int size(jnr.ffi.Runtime r) {
        return r.addressSize() + r.findType(TypeAlias.size_t).size();
    }
}
