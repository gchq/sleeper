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

/**
 * A Sleeper row key.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
public class FFIElement extends Struct {
    public final Struct.Enum<FFIElementType> contained = new Struct.Enum<>(FFIElementType.class);
    public final FFIElementData item = inner(FFIElementData.class);

    public FFIElement(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    public FFIElement(jnr.ffi.Runtime runtime, Object o) {
        this(runtime);
        set(o);
    }

    /**
     * Set the contents of this element.
     *
     * @param  o                        the object
     * @throws IllegalArgumentException if o is not of valid type
     */
    public void set(Object o) {
        if (o == null) {
            contained.set(FFIElementType.Empty);
        } else {
            if (o instanceof Integer) {
                int v = (Integer) o;
                contained.set(FFIElementType.Int32);
                item.set(v);
            } else if (o instanceof Long) {
                long v = (Long) o;
                contained.set(FFIElementType.Int64);
                item.set(v);
            } else if (o instanceof java.lang.String) {
                java.lang.String v = (java.lang.String) o;
                contained.set(FFIElementType.String);
                item.set(v);
            } else if (o instanceof byte[]) {
                byte[] v = (byte[]) o;
                contained.set(FFIElementType.ByteArray);
                item.set(v);
            } else {
                throw new IllegalArgumentException("can't accept " + o.getClass());
            }
        }
    }

    /**
     * Retrieve contents.
     *
     * @return the contained element
     */
    public Object get() {
        return switch (contained.get()) {
            case Empty -> null;
            case Int32 -> item.getInt();
            case Int64 -> item.getLong();
            case String -> item.getString();
            case ByteArray -> item.getBytes();
        };
    }
}
