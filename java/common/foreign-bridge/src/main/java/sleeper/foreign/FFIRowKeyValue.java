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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jnr.ffi.Struct;

/**
 * A value for a Sleeper row key. Row keys columns may be several different types, e.g. int, long, etc. This class is a
 * strongly typed FFI compatible representation. It consists of the row key data in a C union type and an enum member
 * specifying the type.
 *
 * <strong>THIS IS A C COMPATIBLE FFI STRUCT!</strong> If you updated this struct (field ordering, types, etc.),
 * you MUST update the corresponding Rust definition in rust/sleeper_df/src/objects.rs. The order and types of
 * the fields must match exactly.
 */
@SuppressFBWarnings({"PA_PUBLIC_MUTABLE_OBJECT_ATTRIBUTE"})
public class FFIRowKeyValue extends Struct {
    public final Struct.Enum<FFIRowKeyValueType> contained = new Struct.Enum<>(FFIRowKeyValueType.class);
    public final FFIRowKeyValueData item = inner(FFIRowKeyValueData.class);

    public FFIRowKeyValue(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    public FFIRowKeyValue(jnr.ffi.Runtime runtime, Object o) {
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
            contained.set(FFIRowKeyValueType.Empty);
        } else {
            if (o instanceof Integer) {
                int v = (Integer) o;
                contained.set(FFIRowKeyValueType.Int32);
                item.set(v);
            } else if (o instanceof Long) {
                long v = (Long) o;
                contained.set(FFIRowKeyValueType.Int64);
                item.set(v);
            } else if (o instanceof java.lang.String) {
                java.lang.String v = (java.lang.String) o;
                contained.set(FFIRowKeyValueType.String);
                item.set(v);
            } else if (o instanceof byte[]) {
                byte[] v = (byte[]) o;
                contained.set(FFIRowKeyValueType.ByteArray);
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
