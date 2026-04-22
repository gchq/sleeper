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
import jnr.ffi.Union;

import java.nio.charset.StandardCharsets;

/**
 * Java mapping of union type for containing a Sleeper row key.
 * Unions in C use overlapping storage for their members. Only
 * one member may be "active" at a time.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
public class FFIElementData extends Union {
    public final Struct.Signed32 int32 = new Struct.Signed32();
    public final Struct.Signed64 int64 = new Struct.Signed64();
    public final Struct.StructRef<FFIBytes> string = new Struct.StructRef<>(FFIBytes.class);
    public final Struct.StructRef<FFIBytes> bytes = new Struct.StructRef<>(FFIBytes.class);
    /** Prevent GC. */
    FFIBytes java_holder;

    public FFIElementData(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    /**
     * Set contents to an int.
     *
     * @param value element value
     */
    public void set(int value) {
        int32.set(value);
        java_holder = null;
    }

    /**
     * Set contents to a long.
     *
     * @param value element value
     */
    public void set(long value) {
        int64.set(value);
        java_holder = null;
    }

    /**
     * Set contents to a string.
     *
     * @param value element value
     */
    public void set(java.lang.String value) {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        FFIBytes contents = new FFIBytes(getRuntime(), data);
        string.set(contents);
        java_holder = contents;
    }

    /**
     * Set contents to a byte array.
     *
     * @param value element value
     */
    public void set(byte[] value) {
        FFIBytes contents = new FFIBytes(getRuntime(), value);
        bytes.set(contents);
        java_holder = contents;
    }
}
