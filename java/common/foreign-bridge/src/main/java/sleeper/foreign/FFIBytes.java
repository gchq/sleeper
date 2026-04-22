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

import java.util.Objects;

/**
 * Java implementation of FFIBytes. A simple length and pointer to a data buffer.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressWarnings(value = {"checkstyle:membername"})
public class FFIBytes extends Struct {
    /** Length of stored data. */
    public final Struct.size_t length = new Struct.size_t();
    /** Data buffer. */
    public final Struct.Pointer data = new Struct.Pointer();
    /**
     * Memory address of byte array in memory. Stored to prevent GC until this object is collected.
     */
    private jnr.ffi.Pointer nativePointer;

    public FFIBytes(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    public FFIBytes(jnr.ffi.Runtime runtime, byte[] buffer) {
        this(runtime);
        setData(buffer);
    }

    public void setData(byte[] buffer) {
        Objects.requireNonNull(buffer, "buffer");
        // Allocate some memory for the data
        this.nativePointer = getRuntime().getMemoryManager().allocateDirect(buffer.length);
        this.nativePointer.put(0, buffer, 0, buffer.length);
        this.length.set(buffer.length);
        this.data.set(nativePointer);
    }

    /**
     * Retrieve a Java copy of the internal data buffer. This is a snapshot of the data.
     *
     * @return copy of internal byte array
     */
    public byte[] getData() {
        if (nativePointer == null) {
            throw new IllegalStateException("no data in buffer");
        }
        byte[] result = new byte[length.intValue()];
        nativePointer.get(0, result, 0, length.intValue());
        return result;
    }
}
